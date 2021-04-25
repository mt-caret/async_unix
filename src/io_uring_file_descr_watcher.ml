open Core
open Import
open File_descr_watcher_intf
open Read_write.Export
module Table = Bounded_int_table

module Flags = struct
  include Io_uring.Poll_flags

  let in_out = in_ + out
end

type t =
  { io_uring: (int Io_uring.t [@sexp.opaque])
  ; rearm_polling : user_data:int -> res:int -> flags:int -> unit
  ; handle_fd_read_ready : user_data:int -> res:int -> flags:int -> unit
  ; handle_fd_write_ready : user_data:int -> res:int -> flags:int -> unit
  ; flags_by_fd : (File_descr.t, Flags.t) Table.t
  ; tags_by_fd : (File_descr.t, int Io_uring.Tag.t) Table.t
  } [@@deriving sexp_of, fields]

type 'a additional_create_args = 'a

let unpack_file_descr ~user_data =
  Int.bit_and user_data (0xffff_ffff) |> File_descr.of_int
;;

let unpack_flags ~user_data =
  Int.shift_right user_data 32
  |> Io_uring.Poll_flags.of_int
;;

let pack_user_data ~file_descr ~flags =
  let user_data =
    Int.shift_left (Io_uring.Poll_flags.to_int_exn flags) 32
    |> Int.bit_or (File_descr.to_int file_descr)
  in
  (* print_s [%message "packed user_data" (user_data : int) (file_descr : File_descr.t) (flags : Flags.t)]; *)
  user_data
;;

let create ~num_file_descrs ~handle_fd_read_ready ~handle_fd_write_ready =
  let max_submission_entries =
    Io_uring_max_submission_entries.raw Config.io_uring_max_submission_entries
  in
  let max_completion_entries =
    Io_uring_max_completion_entries.raw Config.io_uring_max_completion_entries
  in
  let io_uring = Io_uring.create ~max_submission_entries ~max_completion_entries in
  let flags_by_fd =
    Table.create
      ~num_keys:num_file_descrs
      ~key_to_int:File_descr.to_int
      ~sexp_of_key:File_descr.sexp_of_t
      ()
  in
  let tags_by_fd =
    Table.create
      ~num_keys:num_file_descrs
      ~key_to_int:File_descr.to_int
      ~sexp_of_key:File_descr.sexp_of_t
      ()
  in
  { io_uring
  ; rearm_polling = (fun ~user_data ~res ~flags:_ ->
    let file_descr = unpack_file_descr ~user_data in
    let flags = unpack_flags ~user_data in
    (* print_s [%message "rearm-polling" (user_data : int) (file_descr : File_descr.t) (flags : Flags.t) (flags_by_fd : (File_descr.t, Flags.t) Table.t)]; *)
    if Table.mem flags_by_fd file_descr then (
      (* print_s [%message "rearming poll" (file_descr : File_descr.t) (flags : Flags.t)]; *)
      let tag =
        Io_uring.prepare_poll_add io_uring Io_uring.Sqe_flags.none  file_descr flags user_data
      in
      let sq_full = Io_uring.Tag.Option.is_none tag in
      if sq_full then
        raise_s
          [%message
            "Io_uring_file_descr_watcher.thread_safe_check: submission queue is full"]))
  ; handle_fd_read_ready = (fun ~user_data ~res ~flags:_ ->
    let file_descr = unpack_file_descr ~user_data in
    (* print_s [%message "handle-fd-read-ready" (file_descr : File_descr.t)]; *)
    if Table.mem flags_by_fd file_descr then (
      let flags = Flags.of_int res in
      if Flags.do_intersect flags Flags.in_ then
        handle_fd_read_ready file_descr))
  ; handle_fd_write_ready = (fun ~user_data ~res ~flags:_ ->
    let file_descr = unpack_file_descr ~user_data in
    (* print_s [%message "handle-fd-write-ready" (file_descr : File_descr.t)]; *)
    if Table.mem flags_by_fd file_descr then (
      let flags = Flags.of_int res in
      if Flags.do_intersect flags Flags.out then
        handle_fd_write_ready file_descr))
  ; flags_by_fd
  ; tags_by_fd
  }
;;

let reset_in_forked_process t = Io_uring.close t.io_uring

let backend = Config.File_descr_watcher.Io_uring

(* TOIMPL: ensure flags_by_fd and tags_by_fd stay in sync *)
let invariant t : unit =
  try
    Fields.iter
      ~io_uring:ignore
      ~rearm_polling:ignore
      ~handle_fd_read_ready:ignore
      ~handle_fd_write_ready:ignore
      ~flags_by_fd:ignore
      ~tags_by_fd:ignore
  with
  | exn ->
    raise_s
      [%message
        "Io_uring_file_descr_watcher.invariant failed"
        (exn : exn)
        ~io_uring_file_descr_watcher:(t : t)]
;;

let set_tags_by_fd_or_raise t file_descr tag =
  match%optional.Io_uring.Tag.Option tag with
  | None ->
    raise_s
      [%message
        "Io_uring_file_descr_watcher.set: submission queue is full"
        (t : t)]
  | Some tag ->
      Table.set t.tags_by_fd ~key:file_descr ~data:tag
;;

(* TOIMPL: maybe instead of raising when sq is full, we should submit and retry? *)
let set t file_descr desired =
  (* print_s [%message "set: " (file_descr : File_descr.t) (desired : bool Read_write.t)]; *)
  let actual_flags = Table.find t.flags_by_fd file_descr in
  let desired_flags =
    match desired.read, desired.write with
    | false, false -> None
    | true, false -> Some Flags.in_
    | false, true -> Some Flags.out
    | true, true -> Some Flags.in_out
  in
  match actual_flags, desired_flags with
  | None, None -> ()
  | None, Some d ->
      Table.set t.flags_by_fd ~key:file_descr ~data:d;
      pack_user_data ~file_descr ~flags:d
      |> Io_uring.prepare_poll_add t.io_uring Io_uring.Sqe_flags.none  file_descr d
      |> set_tags_by_fd_or_raise t file_descr
  | Some a, None ->
      Table.remove t.flags_by_fd file_descr;
      let sq_full =
        Table.find_exn t.tags_by_fd file_descr
        |> Io_uring.prepare_poll_remove t.io_uring Io_uring.Sqe_flags.none
      in
      if sq_full then
        raise_s
          [%message
            "Io_uring_file_descr_watcher.set: submission queue is full"
            (t : t)];
      Table.remove t.tags_by_fd file_descr
  | Some a, Some d ->
      if not (Flags.equal a d) then (
        Table.set t.flags_by_fd ~key:file_descr ~data:d;
        pack_user_data ~file_descr ~flags:d
        |> Io_uring.prepare_poll_add t.io_uring Io_uring.Sqe_flags.none file_descr d
        |> set_tags_by_fd_or_raise t file_descr)
;;

let iter t ~f =
  Table.iteri t.flags_by_fd ~f:(fun ~key:file_descr ~data:flags ->
    if Flags.do_intersect flags Flags.in_ then f file_descr `Read;
    if Flags.do_intersect flags Flags.out then f file_descr `Write)
;;

module Pre = struct
  type t = unit [@@deriving sexp_of]
end

let pre_check _t = ()

module Check_result = struct
  type t = unit [@@deriving sexp_of]
end

let io_uring_wait (type a) (io_uring : _ Io_uring.t) (timeout : a Timeout.t) (span_or_unit : a) =
  match timeout with
  | Never -> 
      let ret = Io_uring.submit io_uring in
      if ret < 0 then
        Unix.unix_error (-ret) "Io_uring_file_descr_watcher.io_uring_wait" "`Never";
      Io_uring.wait io_uring ~timeout:`Never
  | Immediately ->
      let ret = Io_uring.submit io_uring in
      if ret < 0 then
        Unix.unix_error (-ret) "Io_uring_file_descr_watcher.io_uring_wait" "`Immediately";
      Io_uring.wait io_uring ~timeout:`Immediately
  | After -> Io_uring.wait_timeout_after io_uring span_or_unit
;;

let thread_safe_check t () timeout span_or_unit =
  (* print_s [%message "thread_safe_check: " (t.flags_by_fd : (File_descr.t, Flags.t) Table.t)]; *)
  io_uring_wait t.io_uring timeout span_or_unit

let post_check t (check_result : Check_result.t) =
  Io_uring.iter_completions t.io_uring ~f:t.rearm_polling;
  Io_uring.iter_completions t.io_uring ~f:t.handle_fd_write_ready;
  Io_uring.iter_completions t.io_uring ~f:t.handle_fd_read_ready;
  Io_uring.clear_completions t.io_uring;
;;
