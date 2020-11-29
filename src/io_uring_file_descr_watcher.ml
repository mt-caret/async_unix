open Core
open Import
open File_descr_watcher_intf
open Read_write.Export
module Io_uring = Linux_ext.Io_uring
module Table = Bounded_int_table

module Flags = struct
  include Io_uring.Flags

  let in_out = in_ + out
end

type t =
  { io_uring: ([ `Poll ] Io_uring.t [@sexp.opaque])
  ; handle_fd_read_ready : File_descr.t -> unit
  ; handle_fd_write_ready : File_descr.t -> unit
  ; flags_by_fd : (File_descr.t, Flags.t) Table.t;
  } [@@deriving sexp_of, fields]

type 'a additional_create_args = 'a

let create ~num_file_descrs ~handle_fd_read_ready ~handle_fd_write_ready =
  let max_submission_entries =
    Io_uring_max_submission_entries.raw Config.io_uring_max_submission_entries
    |> Int63.of_int
  in
  { io_uring = Io_uring.create ~max_submission_entries
  ; handle_fd_read_ready
  ; handle_fd_write_ready
  ; flags_by_fd =
      Table.create
        ~num_keys:num_file_descrs
        ~key_to_int:File_descr.to_int
        ~sexp_of_key:File_descr.sexp_of_t
        ()
  }
;;

let reset_in_forked_process t = Io_uring.close t.io_uring

let backend = Config.File_descr_watcher.Io_uring

(* TOIMPL: no invariants, really? *)
let invariant t : unit =
  try
    Fields.iter
      ~io_uring:ignore
      ~handle_fd_read_ready:ignore
      ~handle_fd_write_ready:ignore
      ~flags_by_fd:ignore
  with
  | exn ->
    raise_s
      [%message
        "Io_uring_file_descr_watcher.invariant failed"
        (exn : exn)
        ~io_uring_file_descr_watcher:(t : t)]
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
      let sq_full = Io_uring.poll_add t.io_uring file_descr d in
      if sq_full then
        raise_s
          [%message
            "Io_uring_file_descr_watcher.set: submission queue is full"
            (t : t)];
  | Some a, None ->
      Table.remove t.flags_by_fd file_descr;
      let sq_full = Io_uring.poll_remove t.io_uring file_descr a in
      if sq_full then
        raise_s
          [%message
            "Io_uring_file_descr_watcher.set: submission queue is full"
            (t : t)];
  | Some a, Some d ->
      if not (Flags.equal a d) then (
        Table.set t.flags_by_fd ~key:file_descr ~data:d;
        let sq_full = Io_uring.poll_add t.io_uring file_descr d in
        if sq_full then
          raise_s
            [%message
              "Io_uring_file_descr_watcher.set: submission queue is full"
              (t : t)])
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
  type t = [ `Poll ] Io_uring.Cqe.t list [@@deriving sexp_of]
end

let io_uring_wait (type a) (io_uring : _ Io_uring.t) (timeout : a Timeout.t) (span_or_unit : a) =
  match timeout with
  | Never -> 
      let _num_submitted = Io_uring.submit io_uring in
      (* print_s [%message "submitted (timeout:Never)" (num_submitted : Int63.t)]; *)
      Io_uring.wait io_uring ~timeout:`Never
  | Immediately ->
      let _num_submitted = Io_uring.submit io_uring in
      (* print_s [%message "submitted (timeout:Immediately)" (num_submitted : Int63.t)]; *)
      Io_uring.wait io_uring ~timeout:`Immediately
  | After -> Io_uring.wait_timeout_after io_uring span_or_unit
;;

let thread_safe_check t () timeout span_or_unit =
  (* print_s [%message "thread_safe_check: " (t.flags_by_fd : (File_descr.t, Flags.t) Table.t)]; *)
  let cqe_list = io_uring_wait t.io_uring timeout span_or_unit in
  List.filter cqe_list ~f:(fun cqe ->
    let file_descr = (Io_uring.User_data.file_descr cqe.user_data) in
    let still_in_table = Table.mem t.flags_by_fd file_descr in
    if still_in_table then (
      let flags = (Io_uring.User_data.flags cqe.user_data) in
      (* print_s [%message "rearming poll" (file_descr : File_descr.t) (flags : Flags.t)]; *)
      let sq_full =
        Io_uring.poll_add t.io_uring file_descr flags
      in
      if sq_full then
        raise_s
          [%message
            "Io_uring_file_descr_watcher.thread_safe_check: submission queue is full"
            (t : t)]);
      (* else
        print_s [%message "skipping rearm as no longer in table"]; *)
      still_in_table)

let post_check t (check_result : Check_result.t) =
  List.iter check_result ~f:(fun cqe ->
    let flags = Int63.to_int_exn cqe.ret |> Flags.of_int in
    if Flags.do_intersect flags Flags.out then
      Io_uring.User_data.file_descr cqe.user_data
        |> t.handle_fd_write_ready);
  List.iter check_result ~f:(fun cqe ->
    let flags = Int63.to_int_exn cqe.ret |> Flags.of_int in
    if Flags.do_intersect flags Flags.in_ then
      Io_uring.User_data.file_descr cqe.user_data
        |> t.handle_fd_read_ready)
;;
