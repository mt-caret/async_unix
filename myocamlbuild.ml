(* OASIS_START *)
(* OASIS_STOP *)

let dispatch = function
  | After_rules ->
    List.iter
      (fun tag ->
         pflag ["ocaml"; tag] "pa_ounit_lib"
           (fun s -> S[A"-ppopt"; A"-pa-ounit-lib"; A"-ppopt"; A s]))
      ["ocamldep"; "compile"; "doc"];

    flag ["c"; "compile"] & S[A"-I"; A"lib"; A"-package"; A"core"; A"-thread"]
| _ ->
    ()

let () = Ocamlbuild_plugin.dispatch (fun hook -> dispatch hook; dispatch_default hook)
