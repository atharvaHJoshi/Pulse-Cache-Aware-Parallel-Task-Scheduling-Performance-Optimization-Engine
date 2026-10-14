(* Pulse reference model.

   A small, dependency-free reference implementation of the DAG analyses and
   list-scheduling simulation used by the C++ engine.  It exists so the C++
   scheduler can be differentially tested against an independent, easily
   auditable model:

     * the topological order validity check,
     * bottom-level (critical-path-to-sink) and total critical path, and
     * an ideal-cost list-schedule makespan for FIFO and critical-path policies.

   Input format (produced by `pulse_bench --dump-dag <name>`), one record per
   line; blank lines and lines starting with '#' are ignored:

       tasks <n>
       cost   <id> <cost>
       edge   <from> <to>
       region <id> <data> <write>      (ignored by this model)

   Output is line oriented so shell tests can diff selected fields:

       n <n>
       topo_ok true|false
       critical_path <n>
       lower_bound <n>
       sim_fifo <n>
       sim_cp <n>
       bottom <id> <n>            (one per task, id order)
*)

(* ---- priority structures ------------------------------------------------- *)

(* Max-first set keyed by (priority, id): larger priority first, and on ties
   the *smallest* id first, matching the C++ critical-path tie-break. *)
module Prio = Set.Make (struct
  type t = int * int

  let compare (p1, i1) (p2, i2) =
    let c = Stdlib.compare p1 p2 in
    if c <> 0 then c else Stdlib.compare i2 i1
end)

(* Min-first set of scheduled finish events, keyed by (finish_time, task_id). *)
module Ev = Set.Make (struct
  type t = int * int

  let compare = Stdlib.compare
end)

(* ---- parsing ------------------------------------------------------------- *)

type node = {
  id : int;
  cost : int;
  succ : int list ref;
  indeg : int ref;
}

let strip s =
  let n = String.length s in
  let rec left i = if i < n && (s.[i] = ' ' || s.[i] = '\t') then left (i + 1) else i in
  let rec right i = if i >= 0 && (s.[i] = ' ' || s.[i] = '\t' || s.[i] = '\r') then right (i - 1) else i in
  let a = left 0 and b = right (n - 1) in
  if b < a then "" else String.sub s a (b - a + 1)

let fields s =
  strip s |> String.split_on_char ' ' |> List.filter (fun x -> x <> "")

let parse_file path =
  let ic = open_in path in
  let lines = ref [] in
  (try
     while true do
       lines := input_line ic :: !lines
     done
   with End_of_file -> ());
  close_in ic;
  List.rev !lines

(* ---- analysis ------------------------------------------------------------ *)

let () =
  if Array.length Sys.argv < 2 then (
    prerr_endline "usage: pulse_ref <dag-file>";
    exit 2);
  let lines = parse_file Sys.argv.(1) in

  (* Pass 1: node count. *)
  let n = ref 0 in
  List.iter
    (fun line ->
      match fields line with
      | [ "tasks"; x ] -> n := int_of_string x
      | _ -> ())
    lines;

  let nodes = Array.init (max 1 !n) (fun i -> { id = i; cost = 0; succ = ref []; indeg = ref 0 }) in
  let edges = ref [] in

  (* Pass 2: costs and edges. *)
  List.iter
    (fun line ->
      match fields line with
      | [ "cost"; id; c ] ->
          let id = int_of_string id in
          nodes.(id) <- { nodes.(id) with cost = int_of_string c }
      | [ "edge"; u; v ] -> edges := (int_of_string u, int_of_string v) :: !edges
      | _ -> ())
    lines;

  let edges = List.rev !edges in
  List.iter
    (fun (u, v) ->
      nodes.(u).succ := v :: !(nodes.(u).succ);
      incr nodes.(v).indeg)
    edges;

  (* Kahn topological sort. *)
  let queue = Queue.create () in
  Array.iter (fun nd -> if !(nd.indeg) = 0 then Queue.add nd.id queue) nodes;
  let topo = ref [] in
  while not (Queue.is_empty queue) do
    let u = Queue.take queue in
    topo := u :: !topo;
    List.iter
      (fun v ->
        decr nodes.(v).indeg;
        if !(nodes.(v).indeg) = 0 then Queue.add v queue)
      !(nodes.(u).succ)
  done;
  let topo = List.rev !topo in
  let topo_ok = List.length topo = !n in

  (* Bottom level: cost[u] + max over successors of bottom[succ], inclusive. *)
  let bottom = Array.make (max 1 !n) 0 in
  if topo_ok then
    List.iter
      (fun u ->
        let best = List.fold_left (fun acc v -> max acc bottom.(v)) 0 !(nodes.(u).succ) in
        bottom.(u) <- nodes.(u).cost + best)
      (List.rev topo);

  (* Recompute the source set from the graph: a node is a source iff nothing
     lists it as a successor. *)
  let has_pred = Array.make (max 1 !n) false in
  Array.iter (fun nd -> List.iter (fun v -> has_pred.(v) <- true) !(nd.succ)) nodes;
  let total_cp =
    Array.fold_left
      (fun acc nd -> if has_pred.(nd.id) then acc else max acc bottom.(nd.id))
      0 nodes
  in

  let total_cost = Array.fold_left (fun acc nd -> acc + nd.cost) 0 nodes in

  (* ---- ideal list-schedule simulation ------------------------------------ *)

  (* Rebuild indegrees for simulation. *)
  let mk_indeg () =
    let d = Array.make (max 1 !n) 0 in
    Array.iter (fun nd -> List.iter (fun v -> d.(v) <- d.(v) + 1) !(nd.succ)) nodes;
    d
  in

  let simulate ~workers ~use_priority =
    let indeg = mk_indeg () in
    let fifoq = Queue.create () in
    let prio = ref Prio.empty in
    let add_ready id =
      if use_priority then prio := Prio.add (bottom.(id), id) !prio else Queue.add id fifoq
    in
    let pop_ready () =
      if use_priority then
        match Prio.max_elt_opt !prio with
        | None -> None
        | Some ((_, id) as k) ->
            prio := Prio.remove k !prio;
            Some id
      else if Queue.is_empty fifoq then None
      else Some (Queue.take fifoq)
    in
    let ready_count () = if use_priority then Prio.cardinal !prio else Queue.length fifoq in
    Array.iter (fun nd -> if indeg.(nd.id) = 0 then add_ready nd.id) nodes;
    let events = ref Ev.empty in
    let busy = ref 0 in
    let time = ref 0 in
    let makespan = ref 0 in
    let running = ref true in
    while !running do
      (* dispatch while a worker is free and work is ready *)
      while !busy < workers && ready_count () > 0 do
        match pop_ready () with
        | None -> busy := workers (* unreachable, keeps loop honest *)
        | Some id ->
            events := Ev.add (!time + nodes.(id).cost, id) !events;
            incr busy
      done;
      if Ev.is_empty !events then running := false
      else begin
        let (ftime, id) = Ev.min_elt !events in
        events := Ev.remove (ftime, id) !events;
        time := ftime;
        decr busy;
        makespan := max !makespan !time;
        List.iter
          (fun v ->
            indeg.(v) <- indeg.(v) - 1;
            if indeg.(v) = 0 then add_ready v)
          !(nodes.(id).succ)
      end
    done;
    !makespan
  in

  let workers =
    if Array.length Sys.argv >= 3 then int_of_string Sys.argv.(2) else 1
  in

  Printf.printf "n %d\n" !n;
  Printf.printf "topo_ok %b\n" topo_ok;
  Printf.printf "critical_path %d\n" total_cp;
  let lb = if workers <= 0 then 0 else (total_cost + workers - 1) / workers in
  Printf.printf "lower_bound %d\n" (max lb total_cp);
  Printf.printf "sim_fifo %d\n" (simulate ~workers ~use_priority:false);
  Printf.printf "sim_cp %d\n" (simulate ~workers ~use_priority:true);
  Array.iter (fun nd -> Printf.printf "bottom %d %d\n" nd.id bottom.(nd.id)) nodes
