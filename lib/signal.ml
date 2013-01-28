open Core.Std
open Import

include Core.Std.Signal

let handle_default `Do_not_use_with_async   = assert false
let ignore         `Do_not_use_with_async   = assert false
let set            `Do_not_use_with_async _ = assert false
let signal         `Do_not_use_with_async _ = assert false

module Scheduler = Raw_scheduler

let the_one_and_only = Scheduler.the_one_and_only

let handle ?stop ts ~f =
  let scheduler = the_one_and_only ~should_lock:true in
  let signal_manager = scheduler.Scheduler.signal_manager in
  let context = Scheduler.current_execution_context scheduler in
  let handler =
    Raw_signal_manager.install_handler signal_manager ts
      (fun signal ->
        Scheduler.with_execution_context scheduler context ~f:(fun () ->
          try f signal
          with exn -> Monitor.send_exn (Monitor.current ()) exn ~backtrace:`Get))
  in
  Option.iter stop ~f:(fun stop ->
    upon stop (fun () ->
      Raw_signal_manager.remove_handler signal_manager handler));
;;

let terminating = [ alrm; hup; int; term; usr1; usr2 ]

let is_managed_by_async t =
  let scheduler = the_one_and_only ~should_lock:true in
  let signal_manager = scheduler.Scheduler.signal_manager in
  Raw_signal_manager.is_managing signal_manager t
;;
