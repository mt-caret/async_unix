open Core
open Import
module Unix = Unix_syscalls

let argv = (Sys.argv [@warning "-3"])
let get_argv = Sys.get_argv
let executable_name = Sys.executable_name
let wrap1 f x1 = In_thread.run (fun () -> f x1)
let wrap2 f x1 x2 = In_thread.run (fun () -> f x1 x2)

let when_file_changes
      ?(time_source = Time_source.wall_clock ())
      ?(poll_delay = sec 0.5)
      file
  =
  let last_reported_mtime = ref None in
  let reader, writer = Pipe.create () in
  let rec loop () =
    Monitor.try_with
      ~run:
        `Schedule
      ~rest:`Log
      ~extract_exn:true
      (fun () -> Unix.stat file)
    >>> fun stat_result ->
    if not (Pipe.is_closed writer)
    then (
      (match stat_result with
       | Error exn ->
         last_reported_mtime := None;
         Pipe.write_without_pushback writer (Error exn)
       | Ok st ->
         let mtime = st.mtime in
         let should_report =
           match !last_reported_mtime with
           | None -> true
           | Some last_reported_mtime -> not (Time.equal mtime last_reported_mtime)
         in
         if should_report
         then (
           last_reported_mtime := Some mtime;
           Pipe.write_without_pushback writer (Ok mtime)));
      Time_source.after time_source (Time_ns.Span.of_span_float_round_nearest poll_delay)
      >>> loop)
  in
  loop ();
  reader
;;

let chdir = wrap1 Sys.chdir
let command = wrap1 Sys.command
let command_exn = wrap1 Sys.command_exn
let quote = Sys.quote
let getcwd = wrap1 Sys.getcwd
let home_directory = wrap1 Sys.home_directory
let ls_dir = wrap1 Sys.ls_dir
let readdir = wrap1 Sys.readdir
let remove = wrap1 Sys.remove
let rename = wrap2 Sys.rename
let wrap_is f ?follow_symlinks path = In_thread.run (fun () -> f ?follow_symlinks path)
let file_exists = wrap_is Sys.file_exists
let file_exists_exn = wrap_is Sys.file_exists_exn

let when_file_exists ?follow_symlinks ?(poll_delay = sec 0.5) file =
  Deferred.create (fun i ->
    let rec loop () =
      file_exists ?follow_symlinks file
      >>> function
      | `Yes -> Ivar.fill i ()
      | `No -> upon (Clock.after poll_delay) loop
      | `Unknown ->
        raise_s [%message "when_file_exists can not check file" (file : string)]
    in
    loop ())
;;

let is_directory = wrap_is Sys.is_directory
let is_directory_exn = wrap_is Sys.is_directory_exn
let is_file = wrap_is Sys.is_file
let is_file_exn = wrap_is Sys.is_file_exn
let c_int_size = Sys.c_int_size
let execution_mode = Sys.execution_mode
let getenv = Sys.getenv
let getenv_exn = Sys.getenv_exn
let int_size = Sys.int_size
let interactive = Sys.interactive
let ocaml_version = Sys.ocaml_version
let os_type = Sys.os_type
let word_size = Sys.word_size
let opaque_identity = Sys.opaque_identity
let big_endian = Sys.big_endian
