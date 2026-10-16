#!/usr/bin/env python3
"""Tables of the incast study, from run directories holding summary.json.

    analyze.py thresholds RUN_DIR... --scope SCOPE   first threshold from healthy reports
    analyze.py gain RUN_DIR...                       fraction of all-top reports per run
    analyze.py binding RUN_DIR...                    degraded spine's rate toward leaf 0
    analyze.py compare --runs RUNS.json --out-root DIR [--stage S]  per-cell arm table
                                                     (all-to-all runs add per-iteration statistics)
    analyze.py calibrate --runs RUNS.json --out-root DIR [--into FILE]   thresholds per scope and K
    analyze.py gains --runs RUNS.json --out-root DIR --into FILE        gain per scope and K
    analyze.py select --runs RUNS.json --out-root DIR [--stage t_tuning] tuning selection
    analyze.py cost --runs RUNS.json --out-root DIR --reference ARM [--stage S]  transient cost table
    analyze.py versus A B --runs RUNS.json --out-root DIR               seed-noise verdict per cell
    analyze.py identity --out-root DIR --other-root DIR [--runs RUNS.json]   same key, same timing and events
    analyze.py a2a_gate --runs RUNS.json... --out-root DIR [--json]     Phase 9 oracle-headroom gate, a row per cell
    analyze.py a2a_gate_reps --runs RUNS.json... --out-root DIR [--stage S] [--markdown FILE]
                                                     reps_oracle against reps, a row per cell
    analyze.py a2a_candidates --runs RUNS.json... --out-root DIR [--stage S] [--markdown FILE]
                                                     cost and reaction per arm and cell
    analyze.py a2a_cycles --runs RUNS.json... --out-root DIR [--stage S] [--markdown FILE]
                                                     cost, detection and re-admission per on- and off-period, per arm
    analyze.py a2a_compute_gaps --runs RUNS.json... --out-root DIR [--stage S] [--markdown FILE]
                                                     median healthy iteration and compute gap per run
    analyze.py a2a_sweep --runs RUNS.json... --out-root DIR [--stage S] [--shared-root DIR] [--markdown FILE]
                                                     loss evidence and the hold arms' recovered fraction per cell
    analyze.py a2a_downlink --runs RUNS.json... --out-root DIR [--stage S] [--markdown FILE]
                                                     a2a_candidates with both ends' holds and tail counts per spine
    analyze.py a2a_reaction --runs RUNS.json... --out-root DIR [--stage S] [--markdown FILE]
                                                     each end's first hold, the losses before it and the lead
    analyze.py a2a_ackroute --runs RUNS.json... --out-root DIR [--stage S] [--ack-route R]
                            [--with-root DIR [--with-stage S]] [--markdown FILE]
                                                     a2a_candidates with correct and false sender holds by the
                                                     answers' routing, and the answers lost per spine
    analyze.py a2a_noack --runs RUNS.json... --out-root DIR [--stage S] [--ack-root DIR...] [--markdown FILE]
                                                     each loss-hold arm without ack_loss_rtts against its ack arm
    analyze.py a2a_hostlink --runs RUNS.json... --out-root DIR [--stage S] [--with-root DIR... [--with-stage S]]
                            [--markdown FILE]
                                                     a2a_candidates with the false sender holds under a lossy
                                                     host link and the losses toward its leaf per spine
    analyze.py a2a_downlink_spray --runs RUNS.json... --out-root DIR [--stage S] [--markdown FILE]
                                                     each end's first hold, the losses before it, the tail
                                                     counts and the correct and false holds of both ends
    analyze.py a2a_twoend --runs RUNS.json... --out-root DIR [--stage S] [--with-root DIR [--with-stage S]]
                          [--markdown FILE]
                                                     a2a_candidates with the answer losses, the sender holds
                                                     under spray and both ends' answer holds
    analyze.py a2a_calibrate --runs RUNS.json... --out-root DIR [--into FILE]
                                                     Phase 9 thresholds per scope and (group, size);
                                                     FILE defaults to incast/a2a_calibration.json

Phase 9 (all-to-all runs, those with a group): cost pairs each faulted run
with the healthy run of the same arm and setting, group, size, iteration
count, fixture (the run's fixture entry, absent on the generator's
defaults) and seed, from any stage of the runs files given (the comparison reuses the gate's uniform
and oracle runs). Primary cost is the mean iteration completion minus the
pair's, tail cost the same on the p99 with linear interpolation; recovered
fraction is 1 - cost / cost(uniform) per seed on the same (group, size,
condition) cell. Those cells follow the transient cells in cost's output. The
gate admits a cell when the oracle's mean primary cost is at most half of
uniform's and the oracle beats uniform on primary cost by the seed rule.
a2a_gate_reps reads the a2a_gate_reps stage's runs alone (--stage) and
gives each cell the mean primary cost of reps_oracle and of reps, and the
verdict "data_helps" when reps_oracle beats reps on primary cost by the
seed rule ("seed_rule_not_met" otherwise, None while either arm lacks three
seeds with a known cost).

a2a_candidates gives every arm of the runs given (all stages unless
--stage; the workflow tabulates a2a_candidates, a2a_receiver, a2a_tail,
a2a_ackloss, a2a_candidates_ack, a2a_tail_ack, a2a_tail_ack_4k,
a2a_sweep_4k, a2a_decode_4k, a2a_ackroute_4k, a2a_noack_4k, a2a_hostlink_4k, a2a_ackroute_fwd_4k,
a2a_downlink_spray_4k, a2a_twoend_4k, a2a_hostlink_s_4k, a2a_twoend_base_4k and a2a_twoend_retract_4k each so) per cell its per-seed and
mean primary and tail cost,
and these measures per faulted run:

* reaction_ns: from the fault's onset (100 base RTTs; 0 for structural) to
  the end of the first sender loss interval ending after it in which spine
  3's share of the sends falls below 1/16, from ns3/spine_losses.csv. With
  a destination_leaf column (leaf and auto scopes), or a destination column
  (pair scope, the destination host's node id, read as its leaf), the sends
  read are those that cross a spine toward the faulted link: for an _up
  fault the sends of
  the hosts of leaf 0 toward every other leaf; for a _down fault every
  sender's sends toward leaf 1, those of the hosts of leaf 1 excluded; for
  structural every host's sends toward another leaf (share_basis
  "remote"). Without either column (host scope) the sends are those of the
  hosts of leaf 0 for an _up fault, of the hosts off leaf 1 for a _down
  fault and of every host for structural, toward any destination
  (share_basis "all"). Intervals are the run's loss_interval_ns. None when the run
  writes no trace, as under the uniform draw, reps_oracle and reps_spine
  (a2a_tail's uniform_tail and reps_tail among them; ns3/spine_arrivals.csv holds
  cumulative counters only at the link events, so it gives no share over
  time), or the share never falls below 1/16.
* receiver_reaction_ns: from the onset to the end of the first receiver
  hold interval ending after it in which a receiving host holds spine 3
  (hold not none), from ns3/receiver_holds.csv, over the rows of the
  faulted link: sender_leaf 0 for an _up fault, the receiving hosts of leaf
  1 for a _down fault, every row for structural. None when the run writes
  no receiver hold trace or no such interval comes.
* rto_events: the retransmission timeouts the run's queue pairs took, from
  ns3/rto_events.csv, and rto_per_iteration their count per iteration, an
  event counted in the latest iteration started at or before it (the
  first for one before every start) by summary.json's iteration starts.
  None when the run writes no timeout trace.
* rto_run: the longest run of consecutive retransmission timeouts on one
  connection, a sender host toward a destination host (the trace's rank and
  dst_rank): one queue pair at a time where a pair's message is one flow
  (dataset_splits 1, the settled fixture's), the queue pairs of a pair's
  flows together otherwise. The trace records no acknowledgement, so the
  run is read from the timeouts' spacing as an InfiniBand queue pair's
  retry count would take them: the simulator times each outstanding send
  out on its own, so a timeout less than one RTO (A2A_RTO_NS) after the one
  that last counted belongs to that timeout and does not count; one from
  one to two RTOs after it, as a repair sent at that timeout and lost again
  times out, counts as the next retry; a longer gap ends the run. 0 without
  a timeout, None without the trace. Per arm, rto_run_longest is the
  longest over the seeds and rto_run_over_limit the seeds whose run exceeds
  A2A_RTO_RUN_LIMIT, NCCL's InfiniBand retry count of 7 (the runs' own
  max_retransmission_retries stays 1024).
* early_cost_ns: the sum, over the iterations that start within 500 us of
  the onset, of the iteration's completion minus the paired healthy run's
  completion of the same iteration; infinite when the faulted run is
  incomplete, None when no iteration starts in the window or the healthy
  run lacks one of them.

a2a_cycles reads the runs with a flapping link (a2a_rs3: a "flap" entry of
start_ns, on_ns, period_ns and repeat in runs.json). Occurrence k's
on-period is [start_ns + k period_ns, + on_ns) and its off-period runs from
there to the next occurrence's start. Per faulted run and period:

* cost_ns: the mean, over the iterations that start inside the period (by
  summary.json's iteration starts), of the iteration's completion minus the
  paired healthy run's completion of the same iteration; None when no
  iteration starts in it or the healthy run lacks one of them, infinite
  when the faulted run is incomplete.
* detection_ns (on-periods): from the on-period's start to the end of the
  first sender loss interval with sends ending after it, and by the
  on-period's end, in which spine 3's share of the sends (read as for
  reaction_ns) is below 1/16; None when the run writes no
  ns3/spine_losses.csv or no such interval comes.
* readmission_ns (off-periods): from the off-period's start to the end of
  the first sender loss interval starting at or after it, and ending by
  the next on-period's start, in which spine 3's share of the sends (read
  as for reaction_ns) is above 1/16; None when the run writes no
  ns3/spine_losses.csv or no such interval comes.
* readmission_1ms_ns (off-periods): as readmission_ns, with the share
  taken over the intervals with sends that end in the 1 ms up to and
  including the end of the interval (which may reach before the
  off-period's start) rather than over the interval alone.
* readmission_90pct_1ms_ns (off-periods): as readmission_1ms_ns, with the
  level 0.9 / 8 (90 percent of a share spread evenly over the 8 spines) in
  place of 1/16.

Per arm, share_basis gives each seed's basis ("remote" or "all", as for
reaction_ns; None without a trace).

Per arm the table gives each period's per-seed values and their means over
the seeds that have one.

a2a_compute_gaps reads the healthy runs of a probe stage (--stage,
a2a_probe_4k by default; a2a_decode_probe_4k) and gives each its median
iteration completion t_h, summary.json's nearest-rank p50 as the a2a_probe
stage read it, and the compute gap the later stages at its payload take
from it: two thirds of the median, rounded to a whole microsecond, so that
the hosts are busy 60 percent of the period; for a decode layer exchange
(a run with combine_bytes), the rest of the A2A_LAYER_PERIOD_NS period
after it, rounded up to a whole microsecond. Both are None while the run is
incomplete.

a2a_sweep reads one stage's runs alone (--stage, a2a_sweep_4k by default;
a2a_tail_ack_4k runs the same arms) and gives each (group, size, condition)
cell. A stage that takes some of its cells' runs from another
(profiles.py A2A_SHARED_4K: a2a_sweep_4k from a2a_tail_ack_4k) reads them
from that stage's out root (--shared-root: the directory a collect job
tabulates, runs.json beside the run directories), those of the cells and
healthy pairs both read only (profiles.py a2a_shared_4k). Per cell:

* the expected loss evidence on spine 3 of a host of leaf 0 under even
  spraying, from the runs' fixture: its payload_bytes (1000 B, the
  generator's, when unset) and its dataset_splits, the flows each pair's
  message per collective is sent as (4, the system layer's, when unset),
  for the message and, with combine_bytes, the combine message:
  n_s_pair, the pair's packets per iteration over the 8 spines, and
  n_s_flow, its mean over the pair's flows; the loss rate p; m_flow and m_pair,
  their losses n_s p; and host_evidence, m_pair times the group's hosts off
  leaf 0, the losses a host expects on the spine per iteration (p, the m
  and host_evidence None under a condition with no uplink loss rate);
* per arm, the per-seed and mean cost, the seed range (largest minus
  smallest cost), the per-seed and mean recovered fraction against
  uniform_tail, and spine_losses_per_host_iteration: the losses the sender
  loss trace counts on spine 3 over the rows reaction_ns reads, per host of
  leaf 0 and per iteration of the run, mean over the seeds (None without a
  trace);
* oracle_versus_reps: the seed-rule verdict of reps_oracle_tail against
  reps_tail on cost; R is read only where the oracle beats it;
* ceiling: per hold arm, R = its mean recovered fraction over
  reps_oracle_tail's (None unless the oracle's is positive);
* gaps: per hold arm other than reps_hold_auto_tail_ack (the leaf scope
  and the per-flow hold), reps_hold_auto_tail_ack's mean recovered fraction
  minus the arm's, the arm's mean cost minus reps_hold_auto_tail_ack's, and
  the seed-rule verdict of the two on cost;
* control, on a group of one leaf (EP8, whose sends cross no spine): per
  arm whether the mean cost is within its seed range of zero (its absolute
  value at most the range), and every seed-rule verdict between two arms
  that is not seed noise. None on every other cell.

a2a_downlink reads one stage's runs alone (--stage, a2a_downlink_4k by
default) and gives a2a_candidates' table, with verdicts against reps_tail,
and per faulted run:

* receiver_tail_missing_by_spine and receiver_tail_missing_late_by_spine:
  the sends the receivers' hold counted missing from a tail repair
  request, and those of them that arrived after all, per requested spine,
  from summary.json's transport rows <event>_spine<k>:control;
* receiver_holds and sender_holds: per spine, the hold intervals of
  ns3/receiver_holds.csv (hold not none) and ns3/spine_losses.csv (grade
  0), and the trace's lost column summed (None without the trace); a hold
  interval is a maximal run of one series' consecutive rows that hold, a
  series being a row's columns but its time, counts, grade and hold, read
  in the order the trace writes them;
* false_receiver_holds and false_sender_holds: the hold intervals and the
  lost count on spines other than 3, which no fault of the stage touches.

Per cell, receiver_versus_sender is the seed-rule verdict of
reps_hold_auto_tail_ack_rx against reps_hold_auto_tail_ack on cost.

a2a_ackroute reads one stage's runs alone (--stage, a2a_ackroute_4k by
default) and gives a2a_candidates' table, with verdicts against reps_tail,
and per faulted run:

* ack_route: the routing of the answers (acknowledgements, NACKs and repair
  requests) its holds are read under: --ack-route for every run, else the
  run's setting's ack_route, data_spine when unset.
* answer_drop_up_by_spine and answer_drop_down_by_spine: per spine, the
  answers a link error or a black hole dropped on a leaf-spine link, up
  (from a leaf to the spine) or down, from summary.json's transport rows
  <event>_spine<k>:control.
* sender_hold_reading: the sender hold intervals of ns3/spine_losses.csv
  (grade 0, a hold interval as for a2a_downlink's sender_holds), each
  correct or false, as correct_holds and false_holds, and the rows that
  hold, as correct_held_rows and false_held_rows; None without the trace.
  A hold is a sender's (its leaf, the row's rank's) of one spine toward one
  destination leaf (the destination_leaf column, or the leaf of the
  destination column's host; a row naming no destination, of hold scope
  host, holds toward every leaf). Every hold in a healthy run, and every
  hold of a spine other than 3, is false. Of spine 3, with the faulted link
  leaf 0's uplink (_up) or the downlink to leaf 1 (_down): under
  data_spine, where an answer returns over the spine of the data it
  answers, an _up hold is correct when its sender is on leaf 0 (toward any
  leaf: the data crosses the link) or it is toward leaf 0 (the answers
  cross it), and a _down hold when it is toward leaf 1 (the data) or its
  sender is on leaf 1 (the answers); under spray, where the answers take
  spines their receivers draw, only the data path counts: an _up hold is
  correct when its sender is on leaf 0, a _down hold when it is toward
  leaf 1. A hold toward every leaf is correct only where its sender alone
  makes it so.
* healthy_sender_hold_reading: the same of the paired healthy run, whose
  holds are all false.

Per cell, versus gives the seed-rule verdict on cost of each
ack_route_oracle arm against the arm it adds the key to, and of
reps_hold_auto_tail_ack against reps_hold_auto_tail4 and against
reps_hold_auto_tail_ack_data_spine, the same arm with every answer over its
data's spine; of each oracle_hops forward arm of a2a_ackroute_fwd_4k
against its arm of a2a_ackroute_4k and, under ack_route_oracle, against
reps_oracle_tail_fwd; and of reps_oracle_tail_fwd_route_oracle against
reps_hold_auto_tail_ack_data_spine; each where the cell has both arms.
--with-root adds the runs of another tabulated out root (runs.json beside
the run directories) of one stage (--with-stage, a2a_ackroute_4k by
default), each read from its own root and costed against its own healthy
pair, so that a2a_ackroute_fwd_4k's runs, read with a2a_ackroute_4k's
dispatch, share its cells; the workflow, which tabulates one dispatch,
passes none.

a2a_noack reads one stage's runs alone (--stage, a2a_noack_4k by default),
whose arms are the loss-hold arms of a2a_tail_ack_4k, a2a_sweep_4k and
a2a_decode_4k without ack_loss_rtts (reps_hold_auto_tail4,
reps_hold_leaf_tail4 and reps_flow_hold_tail4, their profiles those of
reps_hold_auto_tail_ack, reps_hold_leaf_tail_ack and
reps_flow_hold_tail_ack but for ack_loss_rtts), and gives a2a_candidates'
table and per cell and no-ack arm its ack arm's per-seed and mean cost on
the same cell, each stage's runs costed against their own healthy pairs,
read from the tabulated out roots of those stages (--ack-root, runs.json
beside the run directories; none in the workflow, which tabulates one
dispatch), the no-ack arm's mean cost less the ack arm's, and the
seed-rule verdict of the no-ack arm against it on cost (None without the
ack arm's runs on the same seeds).

a2a_hostlink reads one stage's runs alone (--stage, a2a_hostlink_4k by
default), whose faults are host-link conditions (host<kind>_<direction>, a
gray loss on the link of host 9, A2A_FAULT_HOST, to its leaf, leaf 1),
which fault no spine: reaction_ns and receiver_reaction_ns are None, and
correct_hold takes every hold for false. It gives a2a_candidates' table,
with verdicts against reps_tail, and per faulted run sender_hold_reading
and healthy_sender_hold_reading as a2a_ackroute gives them, under the
run's setting's ack_route, and host_link (None without
ns3/spine_losses.csv, or for a run of another condition):

* holding_series: every series of the trace that holds (grade 0), a series
  being a sender (rank), a destination leaf (None for a row of hold scope
  host) and a spine, with its hold intervals (holds) and the rows that
  hold (held_rows);
* toward_faulted_leaf, toward_other_leaves and toward_every_leaf: the
  series, holds and held rows of the holding series whose destination
  leaf is the faulted host's, another, or every leaf;
* lost_toward_faulted_leaf_by_spine and lost_by_spine: per spine, the lost
  column summed over the rows toward the faulted host's leaf, and over
  every row, how the access link's losses spread over the spines.

Each faulted run also gives answer_loss_covered, the sends a loss rule
found covered and counted as answer losses (summary.json's transport row
answer_loss_covered:control, None without it, as in a run without
coverage_first); a2a_hostlink_s_4k's runs are read with --stage
a2a_hostlink_s_4k. Per cell, versus
gives the seed-rule verdict on cost of each coverage-first arm of
a2a_hostlink_s_4k against the a2a_hostlink_4k arm it adds the key to,
where the cell has both. --with-root adds the runs of one stage
(--with-stage, a2a_hostlink_4k by default) of other tabulated out roots
(runs.json beside the run directories), each read from its own root and
costed against its own healthy pair, so that a2a_hostlink_s_4k's runs,
read with a2a_hostlink_4k's dispatches, share its cells; the workflow,
which tabulates one dispatch, passes the dispatch's own root when it ran
both stages.

a2a_reaction reads one stage's runs alone (--stage, a2a_reaction_4k by
default) and gives per faulted run, over the trace rows reaction_ns and
receiver_reaction_ns read on spine 3:

* sender_hold_ns and receiver_hold_ns: from the onset to the end of the
  first interval after it that holds the spine, grade 0 in
  ns3/spine_losses.csv and hold not none in ns3/receiver_holds.csv (None
  without the trace or a hold);
* sender_lost_before and receiver_lost_before: the lost column summed over
  those rows up to and including that interval;
* lead_ns: sender_hold_ns minus receiver_hold_ns, how much sooner the
  receiver held than the run's own sender; and lead_iterations, lead_ns
  over the paired healthy run's mean iteration completion (one all-to-all
  at the training sizes; one layer exchange, dispatch, expert compute and
  combine, on the decode cell).

Per cell, receiver_lead_ns and receiver_lead_iterations set the receiver
arm's receiver_hold_ns against the sender arm's sender_hold_ns on each
seed.

a2a_downlink_spray reads one stage's runs alone (--stage,
a2a_downlink_spray_4k by default), whose faults are on the downlink from
spine 3 to leaf 1 and whose answers take spines their receivers draw, and
gives a2a_candidates' table, with verdicts against reps_tail, and per
faulted run:

* sender_hold_ns, sender_lost_before, receiver_hold_ns and
  receiver_lost_before as a2a_reaction gives them: the first hold of spine
  3 toward leaf 1 by the senders off leaf 1, and by the receivers on leaf 1,
  and the losses their rows counted up to it;
* receiver_tail_missing_by_spine and receiver_tail_missing_late_by_spine as
  a2a_downlink gives them;
* sender_hold_reading as a2a_ackroute gives it, under the run's setting's
  ack_route, and receiver_hold_reading, the same of the receiver hold trace
  (hold not none), a series being a receiver (rank), a sender leaf and a
  spine: a receiver's hold of spine 3 for a sender leaf other than its own
  is correct when the receiver is on leaf 1 (_down: the data crosses the
  downlink) or the sender leaf is 0 (_up: the uplink), and every other
  hold, and every hold in a healthy run, is false;
* healthy_sender_hold_reading and healthy_receiver_hold_reading: the same
  of the paired healthy run.

Per cell, versus gives the seed-rule verdict on cost of each receiver arm
against its sender arm, of the receiver arm without ack_loss_rtts against
the one with it, and of reps_hold_auto_tail_ack against
reps_hold_auto_tail4, where the cell has both arms.

a2a_twoend reads one stage's runs alone (--stage, a2a_twoend_4k by
default), whose answers take spines their receivers draw but under the
control (ack_route data_spine), and gives a2a_candidates' table, with
verdicts against reps_hold_auto_tail4, and per faulted run:

* answer_loss_covered, answer_route_lost, answer_probe, answer_route_hold
  and answer_route_release: from summary.json's transport rows
  <event>:control, the sends a loss rule found covered and counted as
  answer losses (coverage_first), the answers found lost by their route's
  numbers, the receivers' probe answers over a held spine and the senders'
  holds and releases of a route of their answers (answer_path_seq); and
  answer_set_probe, answer_set_replace and answer_repath, the receivers'
  probes of their connections' sets and the members they replaced (ack_route
  reverse_set) and their redraws of a connection's spine (ack_route repath);
  and answer_loss_duplicate and timeout_loss_data, the sends a retransmission
  timeout declared whose resend's answer was flagged as a duplicate, answer
  losses, and those told as data losses (duplicate_report); each None
  without the row, as in a run without the key. answer_route_lost_by_spine
  is answer_route_lost per route spine, from the rows
  answer_route_lost_spine<k>:control;
* sender_hold_reading and healthy_sender_hold_reading as a2a_ackroute gives
  them, under spray for every arm, the control's too: only a hold of the
  data's path is correct;
* answer_hold_reading and healthy_answer_hold_reading, from
  ns3/answer_holds.csv (None without it, as in a run without
  answer_path_seq), each row a change of a hold (held 1, or 0 for its end)
  of a sender (side sender, rank) of a route spine of the answers from a
  receiver leaf (leaf), or of a receiver (side receiver, rank) of a spine of
  its answers to a sender leaf (leaf): per side, the holds correct and false
  (correct when the answers from the answering leaf to the answered one over
  the spine cross the faulted link, as correct_receiver_hold takes the data:
  for _up, answers from leaf 0 over spine 3 to another leaf; for _down,
  answers from another leaf over spine 3 to leaf 1; every hold of a healthy
  run is false), the releases and the holds still held at the trace's end;
  and receiver_holds, per sender leaf and spine the receivers held toward,
  whether correct, the receivers that held it, the first hold from the onset,
  the holds, the releases and the receivers still holding it at the end.

Per cell, versus gives the seed-rule verdict on cost of
reps_hold_auto_tail4_cover against reps_hold_auto_tail4, of
reps_hold_auto_tail4_cover_answer_seq against reps_hold_auto_tail4_cover,
reps_hold_auto_tail4_cover_route_oracle and
reps_hold_auto_tail_ack_data_spine, of reps_hold_auto_tail4_cover_dup
against reps_hold_auto_tail4_cover and of reps_hold_auto_tail4_cover_answer_seq
against it, of a2a_twoend_base_4k's
reps_hold_auto_tail4_cover_reverse_set and reps_hold_auto_tail4_cover_repath
against reps_hold_auto_tail4_cover, and of
reps_hold_auto_tail4_cover_answer_seq against each of them, and of
a2a_twoend_retract_4k's reps_hold_auto_tail4_cover_retract against
reps_hold_auto_tail4_cover and reps_hold_auto_tail4_cover_dup and of
reps_hold_auto_tail4_cover_answer_seq against it, where the cell has both
arms. --with-root adds one stage's runs (--with-stage, a2a_twoend_4k
by default) of another tabulated out root, as a2a_ackroute's does, so that
a2a_twoend_base_4k's runs, read with --stage a2a_twoend_base_4k, share
a2a_twoend_4k's cells.

Gain: per scope and K, the largest candidate gain at which at least 99
percent of the healthy reports of every calibration seed give every spine
the top grade, pooled over the seeds; if none reaches 99 percent, the gain
with the largest fraction, flagged. Selection: per redirect arm and tuning
condition, the setting with the lowest collective completion; for the
t_tuning stage, per arm, the setting with the lowest cost.

Cost (transient stages): per (stage, K, kind, geometry, T, arm) cell and
seed, the run's collective completion minus that of the same arm's paired
healthy run (same setting, K, T and seed, no fault); a run that did not
complete costs infinity. Recovered fraction: 1 - cost / cost(uniform), with
uniform on the same cell and seed. Each cell reports the per-seed values,
their means, the seed-noise verdict of every arm against a reference arm on
cost (lower is better) and on recovered fraction (higher is better), and
the means of the per-transient measures of transients.json: detection
latency and restore latency over the occurrences that have one, the
fraction of occurrences detected, and the false-hold time over any healthy
spine.

Thresholds: for every report of a healthy run, each spine's deviation above
the median spine of that report, for the marked and the trimmed fraction
(and, for switch_queue, the marked and the loss fraction). The first of the
congestion thresholds is the 99th percentile of those deviations over all
reports and every metric but the trimmed fraction, the second and third two
and four times it; the trimmed fraction's own 99th percentile gives the
trim thresholds by the same rule. Each first threshold is capped at 0.25,
so the third stays at most one, and the cap is flagged (capped,
trim_capped). A report is one
(time, rank, sequence) group, and under per_connection one (time, rank,
sequence, sender, sender_port) group.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict, deque
from pathlib import Path

TOP_GRADE = 3
BASE_RTT_NS = 12_324
DEGRADE_AT_NS = 100 * BASE_RTT_NS
DEGRADED_SPINE = 3
SPINES = 8


def reports(run: Path):
    groups: dict[tuple, list[dict[str, str]]] = defaultdict(list)
    with (run / "ns3" / "spine_reports.csv").open() as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = (row["time_ns"], row["rank"], row["sequence"], row.get("sender", ""),
                   row.get("sender_port", ""), row.get("source_leaf", ""))
            groups[key].append(row)
    return groups


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return ordered[index]


def scaled_thresholds(first: float) -> tuple[bool, list[float]]:
    """The rule's (t, 2t, 4t) from a first threshold t. The simulator wants
    three increasing fractions in (0, 1], so the 4t can reach 1 and no
    further: a t above a quarter is capped there, and flagged."""
    capped = first > 0.25
    first = min(max(first, 1e-4), 0.25)
    return capped, [round(first, 6), round(2 * first, 6), round(4 * first, 6)]


def thresholds(runs: list[Path], metrics: tuple[str, ...]) -> dict:
    deviations: dict[str, list[float]] = {m: [] for m in metrics}
    count = 0
    for run in runs:
        for rows in reports(run).values():
            count += 1
            for metric in metrics:
                values = [float(r[metric]) for r in rows]
                median = statistics.median(values)
                deviations[metric].extend(max(0.0, v - median) for v in values)
    p99 = {m: percentile(v, 0.99) for m, v in deviations.items()}
    # The trimmed fraction has thresholds of its own; the others share one.
    capped, congestion = scaled_thresholds(max(v for m, v in p99.items() if m != "trim_fraction"))
    result = {
        "reports": count,
        "p99_deviation": p99,
        "capped": capped,
        "congestion_thresholds": congestion,
    }
    if "trim_fraction" in p99:
        result["trim_capped"], result["trim_thresholds"] = scaled_thresholds(p99["trim_fraction"])
    return result


def all_top_fraction(run: Path) -> dict:
    groups = reports(run)
    top = sum(1 for rows in groups.values() if all(int(r["grade"]) == TOP_GRADE for r in rows))
    return {"reports": len(groups), "all_top": top, "fraction": top / len(groups) if groups else None}


def window_rows(run: Path, name: str = "spine_windows.csv"):
    with (run / name).open() as handle:
        return [
            {k: int(v) for k, v in row.items()} for row in csv.DictReader(handle)
        ]


def binding(run: Path, degraded_bps: float) -> dict:
    summary = json.loads((run / "summary.json").read_text())
    end = summary["first_incast_completion_ns"]
    window = summary["window_ns"]
    total = sum(
        r["bytes"]
        for r in window_rows(run)
        if r["spine"] == DEGRADED_SPINE and DEGRADE_AT_NS <= r["start_ns"] and r["start_ns"] + window <= end
    )
    first_window = -(-DEGRADE_AT_NS // window) * window
    last_window = (end // window) * window
    span_ns = max(1, last_window - first_window)
    return {
        "first_incast_completion_ns": end,
        "utilization": total * 8 / (span_ns * 1e-9) / degraded_bps,
    }


def shed_time(run: Path, target_share: float | None, degrade_at_ns: int = DEGRADE_AT_NS) -> float | None:
    """From the degradation until the degraded spine's share of the traffic
    offered to the spines for the receiving leaf falls below the midpoint of
    1/8 and the target, and stays below it for five consecutive windows.
    Offered, not delivered: a half-rate spine delivers at most its fluid
    share whatever the senders do, so its delivered share cannot show them
    shedding."""
    if target_share is None:
        return None
    midpoint = (1 / SPINES + target_share) / 2
    totals: dict[int, int] = defaultdict(int)
    degraded: dict[int, int] = defaultdict(int)
    for r in window_rows(run, "offered_windows.csv"):
        totals[r["start_ns"]] += r["packets"]
        if r["spine"] == DEGRADED_SPINE:
            degraded[r["start_ns"]] += r["packets"]
    starts = sorted(s for s in totals if s >= degrade_at_ns and totals[s] > 0)
    run_start = None
    run_length = 0
    for start in starts:
        if degraded[start] / totals[start] < midpoint:
            if run_length == 0:
                run_start = start
            run_length += 1
            if run_length == 5:
                return run_start - degrade_at_ns
        else:
            run_length = 0
    return None


TARGET_SHARE = {"half_rate": 200 / 3000, "silent": 0.0}


def iteration_stats(summary: dict) -> dict:
    """An all-to-all run's per-iteration completion statistics; nothing for
    any other workload."""
    timing = summary.get("all_to_all")
    if timing is None:
        return {}
    return {
        "iterations_completed": timing["iterations_completed"],
        **{f"iteration_{name}_ns": timing[f"{name}_ns"] for name in ("mean", "p50", "p99", "max")},
    }


def compare(runs_file: Path, out_root: Path, stage: str | None) -> list[dict]:
    listed = json.loads(runs_file.read_text())
    rows = []
    for run in listed:
        if stage and run["stage"] != stage:
            continue
        directory = out_root / run["key"]
        summary_path = directory / "summary.json"
        if not summary_path.exists():
            rows.append({**run, "status": "missing"})
            continue
        summary = json.loads(summary_path.read_text())
        transport = summary["transport"]
        transient = "geometry" in run
        measured = _transients(directory) if transient else None
        rows.append(
            {
                **run,
                "status": "complete" if summary["complete"] else "incomplete",
                "simulator_stop": summary.get("simulator_stop"),
                "completion_ns": summary["collective_completion_ns"],
                "first_incast_ns": summary["first_incast_completion_ns"],
                "trims": sum(v["count"] for k, v in transport.items() if k.startswith("trim_")),
                "shed_ns": None if transient else shed_time(
                    directory,
                    TARGET_SHARE.get(run["condition"]),
                    10 * BASE_RTT_NS if run["stage"] == "short" else DEGRADE_AT_NS,
                ),
                **({} if not transient else transient_means([measured] if measured else [])),
                **iteration_stats(summary),
                "wall_seconds": summary.get("wall_seconds"),
            }
        )
    return rows


def beats(a: list[int], b: list[int]) -> bool:
    """Paired seeds: a beats b by more than seed noise when a is faster on
    every seed and the mean gap exceeds the larger of the two seed ranges."""
    if len(a) != len(b) or not a:
        return False
    if not all(x < y for x, y in zip(a, b)):
        return False
    gap = sum(b) / len(b) - sum(a) / len(a)
    return gap > max(max(a) - min(a), max(b) - min(b))


def versus(runs_file: Path, out_root: Path, arm_a: str, arm_b: str) -> list[dict]:
    """Per cell, the two arms' completions by seed and the verdict. A cell is
    (stage, K, condition), and for the transient stages also (geometry, T),
    so each holds one fault. A run that did not complete counts as slower
    than any that did, and a cell with a missing run has no verdict."""
    cells: dict[tuple, dict[str, dict[int, float]]] = defaultdict(lambda: defaultdict(dict))
    for run in json.loads(runs_file.read_text()):
        if run["arm"] not in (arm_a, arm_b):
            continue
        path = out_root / run["key"] / "summary.json"
        if not path.exists():
            continue
        completion = json.loads(path.read_text())["collective_completion_ns"]
        cell = (run["stage"], run["k"], run["condition"], run.get("geometry"), run.get("t_us"))
        cells[cell][run["arm"]][run["seed"]] = (
            float("inf") if completion is None else completion
        )
    rows = []
    for (stage, k, condition, geometry, t_us), arms in sorted(cells.items()):
        seeds = sorted(set(arms[arm_a]) & set(arms[arm_b]))
        a = [arms[arm_a][x] for x in seeds]
        b = [arms[arm_b][x] for x in seeds]
        complete = len(seeds) == 3
        rows.append(
            {
                "stage": stage, "k": k, "condition": condition,
                **({} if geometry is None else {"geometry": geometry, "t_us": t_us}),
                "seeds": seeds,
                arm_a: a, arm_b: b,
                "mean_gap_ns": (sum(b) - sum(a)) / len(seeds) if seeds else None,
                "verdict": (
                    None if not complete
                    else f"{arm_a} beats {arm_b}" if beats(a, b)
                    else f"{arm_b} beats {arm_a}" if beats(b, a)
                    else "within seed noise"
                ),
            }
        )
    return rows


def run_identity(directory: Path, other: Path) -> list[str]:
    """What differs between two runs' summary.json: the all-to-all
    iteration starts and completions, and each transport event's count."""
    differences = []
    a, b = (json.loads((d / "summary.json").read_text()) for d in (directory, other))
    for name in ("iteration_start_ns", "iteration_completion_ns"):
        if (a.get("all_to_all") or {}).get(name) != (b.get("all_to_all") or {}).get(name):
            differences.append(name)
    events_a, events_b = ({k: v["count"] for k, v in x.get("transport", {}).items()} for x in (a, b))
    for event in sorted(set(events_a) | set(events_b)):
        if events_a.get(event) != events_b.get(event):
            differences.append(f"{event} {events_a.get(event)} -> {events_b.get(event)}")
    return differences


def identity(out_root: Path, other_root: Path, runs_file: Path | None = None) -> list[dict]:
    """Per run key with a summary under both result roots (every key of
    runs_file when given): "identical" when run_identity finds nothing,
    "different" with what differs, "missing" when either root lacks it."""
    def keys(root):
        return {path.parent.name for path in root.glob("*/summary.json")}

    wanted = (sorted(r["key"] for r in json.loads(runs_file.read_text())) if runs_file
              else sorted(keys(out_root) & keys(other_root)))
    rows = []
    for key in wanted:
        if not ((out_root / key / "summary.json").exists() and (other_root / key / "summary.json").exists()):
            rows.append({"key": key, "status": "missing", "differences": []})
            continue
        differences = run_identity(out_root / key, other_root / key)
        rows.append({"key": key, "status": "different" if differences else "identical", "differences": differences})
    return rows


SCOPE_OF_ARM = {
    "graded_per_receiver": "per_receiver",
    "graded_per_connection": "per_connection",
    "graded_switch_queue": "switch_queue",
}


def _listed(runs_file: Path, out_root: Path, stage: str):
    for run in json.loads(runs_file.read_text()):
        directory = out_root / run["key"]
        if run["stage"] == stage and (directory / "summary.json").exists():
            yield run, directory


def calibrate(runs_file: Path, out_root: Path, existing: dict, stage: str = "calibration") -> dict:
    groups: dict[tuple[str, int], list[Path]] = defaultdict(list)
    for run, directory in _listed(runs_file, out_root, stage):
        groups[(SCOPE_OF_ARM[run["arm"]], run["k"])].append(directory)
    result = {scope: dict(values) for scope, values in existing.items()}
    for (scope, k), directories in sorted(groups.items()):
        metrics = ("mark_fraction", "loss_fraction") if scope == "switch_queue" else ("mark_fraction", "trim_fraction")
        entry = thresholds(directories, metrics)
        entry["runs"] = [d.name for d in directories]
        previous = result.setdefault(scope, {}).get(str(k), {})
        if "estimator_gain" in previous:
            entry["estimator_gain"] = previous["estimator_gain"]
            entry["gain_fraction"] = previous.get("gain_fraction")
        result[scope][str(k)] = entry
    return result


def gains(runs_file: Path, out_root: Path, existing: dict) -> dict:
    tops: dict[tuple[str, int, float], list[int]] = defaultdict(lambda: [0, 0])
    for run, directory in _listed(runs_file, out_root, "gain"):
        gain = run["setting"]["estimator_gain"]
        counted = all_top_fraction(directory)
        cell = tops[(SCOPE_OF_ARM[run["arm"]], run["k"], gain)]
        cell[0] += counted["all_top"]
        cell[1] += counted["reports"]
    result = {scope: dict(values) for scope, values in existing.items()}
    for scope, k in sorted({(s, k) for s, k, _ in tops}):
        fractions = {g: t / n for (s, kk, g), (t, n) in tops.items() if s == scope and kk == k and n}
        passing = [g for g, f in fractions.items() if f >= 0.99]
        # The largest passing gain; if none passes, the largest of those with
        # the best fraction, so a scope the gain does not move (a pooled
        # interval always fills its sample count, which then sets the
        # weight) gets the largest candidate rather than an arbitrary one.
        best = max(fractions.values())
        chosen = max(passing) if passing else max(g for g, f in fractions.items() if f == best)
        entry = result.setdefault(scope, {}).setdefault(str(k), {})
        entry["estimator_gain"] = chosen
        entry["gain_fraction"] = fractions[chosen]
        entry["gain_fractions"] = {str(g): f for g, f in sorted(fractions.items())}
        entry["gain_meets_rule"] = bool(passing)
    return result


def select(runs_file: Path, out_root: Path) -> dict:
    best: dict[tuple[str, str], tuple[int, dict]] = {}
    table: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for run, directory in _listed(runs_file, out_root, "tuning"):
        summary = json.loads((directory / "summary.json").read_text())
        completion = summary["collective_completion_ns"]
        table[run["arm"]][run["condition"]].append({"setting": run["setting"], "completion_ns": completion})
        if completion is None:
            continue
        key = (run["arm"], run["condition"])
        if key not in best or completion < best[key][0]:
            best[key] = (completion, run["setting"])
    selection: dict[str, dict] = defaultdict(dict)
    for (arm, condition), (_, setting) in best.items():
        selection[arm][condition] = setting
    return {**selection, "table": table}


def _transients(directory: Path) -> dict | None:
    path = directory / "transients.json"
    return json.loads(path.read_text()) if path.exists() else None


def _mean(values) -> float | None:
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else None


def transient_means(measured: list[dict]) -> dict:
    """Means over the runs' transients.json: detection and restore latency
    over every occurrence that has one, the detected fraction and the
    false-hold time over any healthy spine per run."""
    occurrences = [o for m in measured for o in m["occurrences"]]
    return {
        "detection_ns_mean": _mean(o["detection_ns"] for o in occurrences),
        "detected_fraction_mean": _mean(m["detected_fraction"] for m in measured),
        "restore_ns_mean": _mean(o["restore_ns"] for o in occurrences),
        "false_hold_ns_mean": _mean((m["false_hold"] or {}).get("any_ns") for m in measured),
    }


def _label(run: dict) -> str:
    setting = run.get("setting")
    return run["arm"] if not setting else f"{run['arm']} {json.dumps(setting, sort_keys=True)}"


def costs(rows: list[dict]) -> list[dict]:
    """Every faulted row with its cost against the paired healthy row (same
    stage, arm, setting, K, T and seed). A row holds stage, arm, setting, k,
    kind, geometry, t_us, seed and completion_ns (None when the run did not
    complete); cost is None when the healthy run is missing or incomplete,
    and infinite when the faulted run is incomplete."""
    def pair(row):
        return (row["stage"], _label(row), row["k"], row["t_us"], row["seed"])

    healthy = {pair(r): r["completion_ns"] for r in rows if r["kind"] == "healthy"}
    out = []
    for row in rows:
        if row["kind"] == "healthy":
            continue
        reference = healthy.get(pair(row))
        if reference is None:
            cost = None
        elif row["completion_ns"] is None:
            cost = float("inf")
        else:
            cost = row["completion_ns"] - reference
        out.append({**row, "healthy_ns": reference, "cost_ns": cost})
    return out


def recovered_fraction(cost: float | None, uniform_cost: float | None) -> float | None:
    """1 - cost / cost(uniform); None when either is unknown or uniform has
    no finite positive cost to recover."""
    if cost is None or uniform_cost is None or not 0 < uniform_cost < float("inf"):
        return None
    return 1 - cost / uniform_cost


def verdict(a: list[float], b: list[float], name_a: str, name_b: str, higher_is_better: bool = False) -> str | None:
    """The seed-noise rule of beats on paired seeds; None without three."""
    if len(a) != 3 or len(b) != 3 or any(v is None for v in a + b):
        return None
    if higher_is_better:
        a, b = [-v for v in a], [-v for v in b]
    if beats(a, b):
        return f"{name_a} beats {name_b}"
    if beats(b, a):
        return f"{name_b} beats {name_a}"
    return "within seed noise"


def cost_cells(rows: list[dict], reference: str, uniform: str = "uniform") -> list[dict]:
    """Per (stage, K, kind, geometry, T) cell, per arm: per-seed cost and
    recovered fraction, their means, the verdicts against the reference arm
    and the transient means (rows may carry a transients entry). An arm is
    named bare, with its setting as a field, when it runs one setting in the
    cell, so the reference and uniform match by arm name; an arm with
    several settings (the tuning stage) is named with each setting."""
    cells: dict[tuple, dict[str, dict[int, dict]]] = defaultdict(lambda: defaultdict(dict))
    faulted = costs(rows)
    settings: dict[tuple, set[str]] = defaultdict(set)
    for row in faulted:
        cell = (row["stage"], row["k"], row["kind"], row["geometry"], row["t_us"])
        settings[(cell, row["arm"])].add(json.dumps(row["setting"], sort_keys=True))
    for row in faulted:
        cell = (row["stage"], row["k"], row["kind"], row["geometry"], row["t_us"])
        label = row["arm"] if len(settings[(cell, row["arm"])]) == 1 else _label(row)
        cells[cell][label][row["seed"]] = row
    out = []
    for (stage, k, kind, geometry, t_us), arms in sorted(cells.items()):
        uniform_costs = {seed: r["cost_ns"] for seed, r in arms.get(uniform, {}).items()}
        table = {}
        for label, by_seed in sorted(arms.items()):
            seeds = sorted(by_seed)
            cost = [by_seed[x]["cost_ns"] for x in seeds]
            recovered = [recovered_fraction(by_seed[x]["cost_ns"], uniform_costs.get(x)) for x in seeds]
            table[label] = {
                "setting": by_seed[seeds[0]]["setting"],
                "seeds": seeds,
                "cost_ns": cost,
                "cost_ns_mean": None if None in cost or not cost else sum(cost) / len(cost),
                "recovered": recovered,
                "recovered_mean": None if None in recovered or not recovered else sum(recovered) / len(recovered),
                **transient_means([by_seed[x]["transients"] for x in seeds if by_seed[x].get("transients")]),
            }
        if reference in table:
            base = table[reference]
            for label, entry in table.items():
                if label == reference or entry["seeds"] != base["seeds"]:
                    continue
                entry["versus_reference_cost"] = verdict(entry["cost_ns"], base["cost_ns"], label, reference)
                entry["versus_reference_recovered"] = verdict(
                    entry["recovered"], base["recovered"], label, reference, higher_is_better=True
                )
        out.append({"stage": stage, "k": k, "kind": kind, "geometry": geometry, "t_us": t_us,
                    "reference": reference, "arms": table})
    return out


def transient_rows(runs_file: Path, out_root: Path, stage: str | None) -> list[dict]:
    """The transient stages' runs with their completion and transients.json."""
    rows = []
    for run in json.loads(runs_file.read_text()):
        if "geometry" not in run or (stage and run["stage"] != stage):
            continue
        directory = out_root / run["key"]
        if not (directory / "summary.json").exists():
            continue
        summary = json.loads((directory / "summary.json").read_text())
        rows.append({**run, "completion_ns": summary["collective_completion_ns"],
                     "transients": _transients(directory)})
    return rows


def select_by_cost(rows: list[dict]) -> dict:
    """Per arm, the setting with the lowest mean cost over its seeds."""
    by_setting: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    table: dict[str, list] = defaultdict(list)
    for row in costs(rows):
        key = json.dumps(row["setting"], sort_keys=True)
        by_setting[row["arm"]][key].append(row["cost_ns"])
        table[row["arm"]].append({k: row[k] for k in ("setting", "seed", "completion_ns", "healthy_ns", "cost_ns")})
    selection: dict = {}
    for arm, settings in sorted(by_setting.items()):
        means = {key: sum(c) / len(c) for key, c in settings.items() if None not in c}
        if means:
            selection[arm] = json.loads(min(means, key=lambda key: (means[key], key)))
    return {**selection, "table": table}


# Phase 9: all-to-all cost against the paired healthy run, and the
# oracle-headroom gate.

A2A_ORACLE = "graded_switch_queue"


def linear_quantile(values: list[float], q: float) -> float:
    """The q-th quantile with linear interpolation between order statistics,
    so the p99 of 40 values lies between the two largest."""
    ordered = sorted(values)
    position = q * (len(ordered) - 1)
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def _listed_runs(runs_files) -> list[dict]:
    """The runs of one or more runs.json files, each key once."""
    if isinstance(runs_files, Path):
        runs_files = [runs_files]
    seen: dict[str, dict] = {}
    for path in runs_files:
        for run in json.loads(Path(path).read_text()):
            seen.setdefault(run["key"], run)
    return list(seen.values())


def a2a_rows(runs_files, out_root: Path, stage: str | None = None) -> list[dict]:
    """The Phase 9 all-to-all runs (those with a group) that have a summary,
    with their per-iteration completions; iterations_ns is None when the run
    did not complete every iteration."""
    rows = []
    for run in _listed_runs(runs_files):
        if "group" not in run or (stage and run["stage"] != stage):
            continue
        path = out_root / run["key"] / "summary.json"
        if not path.exists():
            continue
        timing = json.loads(path.read_text()).get("all_to_all") or {}
        complete = bool(timing.get("complete"))
        values = timing.get("iteration_completion_ns", {})
        rows.append({**run, "complete": complete,
                     "iterations_ns": [values[i] for i in sorted(values, key=int)] if complete else None})
    return rows


def _fixture_key(row: dict) -> str:
    """A run's fixture entry as sorted-key JSON, "" on the generator's
    defaults."""
    fixture = row.get("fixture")
    return "" if fixture is None else json.dumps(fixture, sort_keys=True)


def a2a_costs(rows: list[dict]) -> list[dict]:
    """Every faulted all-to-all row with its primary cost (mean iteration
    completion minus the paired healthy run's) and tail cost (the same on the
    linearly interpolated p99), the pair being the healthy run of the same
    arm and setting, group, size, iteration count and seed, from any stage. Costs are None
    when the healthy run is missing or incomplete and infinite when the
    faulted run is incomplete."""
    def pair(row):
        return (_label(row), row["group"], row["size"], row.get("iterations"), _fixture_key(row), row["seed"])

    healthy: dict[tuple, dict] = {}
    for row in rows:
        if row["condition"] != "healthy":
            continue
        if pair(row) in healthy:
            raise ValueError(f"{row['key']} and {healthy[pair(row)]['key']} pair the same runs")
        healthy[pair(row)] = row
    out = []
    for row in rows:
        if row["condition"] == "healthy":
            continue
        reference = healthy.get(pair(row))
        if reference is None or reference["iterations_ns"] is None:
            cost = tail = None
        elif row["iterations_ns"] is None:
            cost = tail = float("inf")
        else:
            cost = statistics.fmean(row["iterations_ns"]) - statistics.fmean(reference["iterations_ns"])
            tail = linear_quantile(row["iterations_ns"], 0.99) - linear_quantile(reference["iterations_ns"], 0.99)
        out.append({**row, "healthy_key": None if reference is None else reference["key"],
                    "cost_ns": cost, "tail_cost_ns": tail})
    return out


def a2a_cost_cells(rows: list[dict], reference: str | None = None, uniform: str = "uniform") -> list[dict]:
    """Per (group, size, condition) cell, and per fixture where runs set one
    (the cell's fixture), per arm: per-seed primary and tail cost and
    recovered fraction against uniform on the same cell and seed, their
    means, and the seed-rule verdicts against the reference arm. Arms are
    named as in cost_cells: bare when the cell runs one setting of it."""
    faulted = a2a_costs(rows)

    def cell_of(row):
        return row["group"], row["size"], row["condition"], _fixture_key(row)

    settings: dict[tuple, set[str]] = defaultdict(set)
    for row in faulted:
        settings[cell_of(row) + (row["arm"],)].add(json.dumps(row["setting"], sort_keys=True))
    cells: dict[tuple, dict[str, dict[int, dict]]] = defaultdict(lambda: defaultdict(dict))
    for row in faulted:
        cell = cell_of(row)
        label = row["arm"] if len(settings[cell + (row["arm"],)]) == 1 else _label(row)
        cells[cell][label][row["seed"]] = row

    def mean(values):
        return None if not values or None in values else sum(values) / len(values)

    out = []
    for (group, size, condition, fixture), arms in sorted(cells.items()):
        uniform_costs = {seed: r["cost_ns"] for seed, r in arms.get(uniform, {}).items()}
        table = {}
        for label, by_seed in sorted(arms.items()):
            seeds = sorted(by_seed)
            cost = [by_seed[x]["cost_ns"] for x in seeds]
            tail = [by_seed[x]["tail_cost_ns"] for x in seeds]
            recovered = [recovered_fraction(c, uniform_costs.get(x)) for c, x in zip(cost, seeds)]
            table[label] = {
                "setting": by_seed[seeds[0]]["setting"],
                "stages": sorted({by_seed[x]["stage"] for x in seeds}),
                "keys": [by_seed[x]["key"] for x in seeds],
                "seeds": seeds,
                "cost_ns": cost,
                "cost_ns_mean": mean(cost),
                "tail_cost_ns": tail,
                "tail_cost_ns_mean": mean(tail),
                "recovered": recovered,
                "recovered_mean": mean(recovered),
            }
        if reference in table:
            base = table[reference]
            for label, entry in table.items():
                if label == reference or entry["seeds"] != base["seeds"]:
                    continue
                entry["versus_reference_cost"] = verdict(entry["cost_ns"], base["cost_ns"], label, reference)
                entry["versus_reference_recovered"] = verdict(
                    entry["recovered"], base["recovered"], label, reference, higher_is_better=True
                )
        out.append({"group": group, "size": size, "condition": condition,
                    **({"fixture": json.loads(fixture)} if fixture else {}), "reference": reference, "arms": table})
    return out


def a2a_gate(cells: list[dict], oracle: str = A2A_ORACLE, uniform: str = "uniform") -> list[dict]:
    """The oracle-headroom gate per cell: admitted when the oracle's mean
    primary cost is at most half of uniform's (uniform's mean positive) and
    the oracle beats uniform on primary cost by the seed rule, on the same
    three seeds. admitted is None (unreadable) while either arm lacks three
    seeds with a known cost."""
    rows = []
    for cell in cells:
        u, o = cell["arms"].get(uniform), cell["arms"].get(oracle)
        row = {"group": cell["group"], "size": cell["size"], "condition": cell["condition"],
               "uniform_cost_ns": None if u is None else u["cost_ns"],
               "oracle_cost_ns": None if o is None else o["cost_ns"],
               "uniform_mean_ns": None, "oracle_mean_ns": None, "oracle_over_uniform": None,
               "half_rule": None, "seed_rule": None, "admitted": None}
        readable = (
            u is not None and o is not None and u["seeds"] == o["seeds"] and len(u["seeds"]) == 3
            and None not in u["cost_ns"] and None not in o["cost_ns"]
        )
        if readable:
            u_mean, o_mean = u["cost_ns_mean"], o["cost_ns_mean"]
            half = 0 < u_mean and o_mean <= u_mean / 2
            seed = beats(o["cost_ns"], u["cost_ns"])
            row.update(uniform_mean_ns=u_mean, oracle_mean_ns=o_mean,
                       oracle_over_uniform=o_mean / u_mean if 0 < u_mean < float("inf") else None,
                       half_rule=half, seed_rule=seed, admitted=half and seed)
        rows.append(row)
    return rows


A2A_REPS_ORACLE = "reps_oracle"


def a2a_gate_reps(cells: list[dict], oracle: str = A2A_REPS_ORACLE, reps: str = "reps") -> list[dict]:
    """Per cell, reps_oracle's and reps's per-seed and mean primary cost and
    the verdict: "data_helps" when reps_oracle beats reps by the seed rule
    on the same three seeds, "seed_rule_not_met" when it does not, None
    while either arm lacks three seeds with a known cost."""
    rows = []
    for cell in cells:
        o, r = cell["arms"].get(oracle), cell["arms"].get(reps)
        row = {"group": cell["group"], "size": cell["size"], "condition": cell["condition"],
               "reps_oracle_cost_ns": None if o is None else o["cost_ns"],
               "reps_cost_ns": None if r is None else r["cost_ns"],
               "reps_oracle_mean_ns": None, "reps_mean_ns": None, "verdict": None}
        readable = (
            o is not None and r is not None and o["seeds"] == r["seeds"] and len(o["seeds"]) == 3
            and None not in o["cost_ns"] and None not in r["cost_ns"]
        )
        if readable:
            row.update(reps_oracle_mean_ns=o["cost_ns_mean"], reps_mean_ns=r["cost_ns_mean"],
                       verdict="data_helps" if beats(o["cost_ns"], r["cost_ns"]) else "seed_rule_not_met")
        rows.append(row)
    return rows


def _us(value) -> str:
    if value is None:
        return "-"
    return "inf" if value == float("inf") else f"{value / 1000:,.1f}"


def gate_reps_markdown(rows: list[dict]) -> str:
    lines = [
        "| group | size | condition | reps_oracle cost us (seeds) | reps cost us (seeds) "
        "| reps_oracle mean us | reps mean us | verdict |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        def seeds(values):
            return "-" if values is None else ", ".join(_us(v) for v in values)

        lines.append(
            f"| {row['group']} | {row['size']} | {row['condition']} | {seeds(row['reps_oracle_cost_ns'])} "
            f"| {seeds(row['reps_cost_ns'])} | {_us(row['reps_oracle_mean_ns'])} | {_us(row['reps_mean_ns'])} "
            f"| {row['verdict'] or 'unreadable'} |"
        )
    return "\n".join(lines) + "\n"


# Reaction measures of a faulted all-to-all run (a2a_candidates).

A2A_SHARE_LIMIT = 1 / 16
A2A_EARLY_WINDOW_NS = 500_000
A2A_HOSTS_PER_LEAF_OF_TRACE = 8
A2A_DEFAULT_LOSS_INTERVAL_NS = 2 * BASE_RTT_NS
# The runs' retransmission timeout (profiles.py's retransmission_timeout_ns)
# and NCCL's InfiniBand retry count, against which rto_run is read: a queue
# pair fails at a run of more consecutive timeouts than it.
A2A_RTO_NS = 10 * BASE_RTT_NS
A2A_RTO_RUN_LIMIT = 7
A2A_WINDOW_NS = 1_000_000
A2A_FULL_SHARE = 0.9 / SPINES


def fault_onset_ns(condition: str) -> int:
    return 0 if condition == "structural" else DEGRADE_AT_NS


def faulted_share_rows(condition: str, columns):
    """The trace rows that read the faulted link's spine share, and the
    basis: "remote" when the trace's columns name a row's destination leaf
    (sends toward a leaf other than the sender's), "all" otherwise (every
    destination). The destination leaf is the destination_leaf column (hold
    scopes leaf and auto), or the leaf of the destination column's host
    node id (hold scope pair; hosts are the first node ids, in leaf
    order)."""
    hosts = A2A_HOSTS_PER_LEAF_OF_TRACE

    def leaf(row):
        return int(row["rank"]) // hosts

    if "destination_leaf" in columns or "destination" in columns:
        def to(row):
            return int(row["destination_leaf"]) if "destination_leaf" in row else int(row["destination"]) // hosts

        if condition == "structural":
            return (lambda row: to(row) != leaf(row)), "remote"
        if condition.endswith("_up"):
            return (lambda row: leaf(row) == 0 and to(row) != 0), "remote"
        return (lambda row: to(row) == 1 and leaf(row) != 1), "remote"
    if condition == "structural":
        return (lambda row: True), "all"
    if condition.endswith("_up"):
        return (lambda row: leaf(row) == 0), "all"
    return (lambda row: leaf(row) != 1), "all"


def share_series(directory: Path, condition: str, interval_ns: int, spine: int = DEGRADED_SPINE):
    """Per sender loss interval index, the sends of faulted_share_rows'
    rows and those on the spine, and the basis; None without a trace."""
    path = directory / "ns3" / "spine_losses.csv"
    if not path.exists():
        return None
    totals: dict[int, int] = defaultdict(int)
    faulted: dict[int, int] = defaultdict(int)
    with path.open() as handle:
        reader = csv.DictReader(handle)
        keep, basis = faulted_share_rows(condition, reader.fieldnames or ())
        for row in reader:
            if not keep(row):
                continue
            index = (int(row["time_ns"]) - 1) // interval_ns
            totals[index] += int(row["sent"])
            if int(row["spine"]) == spine:
                faulted[index] += int(row["sent"])
    return totals, faulted, basis


def first_below(totals: dict, faulted: dict, interval_ns: int, start: int, end: int | None) -> int | None:
    """From start to the end of the first interval with sends ending after
    start (and by end) whose spine share is below A2A_SHARE_LIMIT."""
    for index in sorted(totals):
        last = (index + 1) * interval_ns
        if last <= start or not totals[index]:
            continue
        if end is not None and last > end:
            break
        if faulted[index] / totals[index] < A2A_SHARE_LIMIT:
            return last - start
    return None


def first_above(totals: dict, faulted: dict, interval_ns: int, start: int, end: int | None) -> int | None:
    """From start to the end of the first interval starting at or after
    start, and ending by end, whose spine share is above A2A_SHARE_LIMIT."""
    for index in sorted(totals):
        first, last = index * interval_ns, (index + 1) * interval_ns
        if first >= start and (end is None or last <= end) and totals[index] \
                and faulted[index] / totals[index] > A2A_SHARE_LIMIT:
            return last - start
    return None


def first_window_above(totals: dict, faulted: dict, interval_ns: int, start: int, end: int | None,
                       level: float, window_ns: int = A2A_WINDOW_NS) -> int | None:
    """From start to the end e of the first interval with sends starting at
    or after start, and ending by end, at which the spine share over the
    intervals with sends ending in (e - window_ns, e] is above level."""
    indices = [i for i in sorted(totals) if totals[i]]
    queue: deque[int] = deque()
    sent = on_spine = 0
    for index in indices:
        last = (index + 1) * interval_ns
        queue.append(index)
        sent += totals[index]
        on_spine += faulted[index]
        while queue and (queue[0] + 1) * interval_ns <= last - window_ns:
            gone = queue.popleft()
            sent -= totals[gone]
            on_spine -= faulted[gone]
        if index * interval_ns >= start and (end is None or last <= end) and sent and on_spine / sent > level:
            return last - start
    return None


def reaction(directory: Path, condition: str, interval_ns: int, spine: int = DEGRADED_SPINE) -> dict:
    """reaction_ns (see the module docstring) and the rows' basis."""
    series = None if host_link_condition(condition) else share_series(directory, condition, interval_ns, spine)
    if series is None:
        return {"reaction_ns": None, "share_basis": None}
    totals, faulted, basis = series
    return {"reaction_ns": first_below(totals, faulted, interval_ns, fault_onset_ns(condition), None),
            "share_basis": basis}


def receiver_reaction(directory: Path, condition: str, spine: int = DEGRADED_SPINE) -> dict:
    """receiver_reaction_ns (see the module docstring) and the rows' basis."""
    path = directory / "ns3" / "receiver_holds.csv"
    if not path.exists() or host_link_condition(condition):
        return {"receiver_reaction_ns": None, "receiver_basis": None}
    keep, basis = receiver_share_rows(condition)
    onset = fault_onset_ns(condition)
    ends = []
    with path.open() as handle:
        for row in csv.DictReader(handle):
            end = int(row["time_ns"])
            if end > onset and int(row["spine"]) == spine and row["hold"] != "none" and keep(row):
                ends.append(end)
    return {"receiver_reaction_ns": min(ends) - onset if ends else None, "receiver_basis": basis}


def rto_counts(directory: Path) -> dict:
    """rto_events, rto_per_iteration and rto_run (see the module
    docstring)."""
    path = directory / "ns3" / "rto_events.csv"
    if not path.exists():
        return {"rto_events": None, "rto_per_iteration": None, "rto_run": None}
    timing = json.loads((directory / "summary.json").read_text()).get("all_to_all") or {}
    starts = sorted((v, int(i)) for i, v in (timing.get("iteration_start_ns") or {}).items())
    per_iteration = {i: 0 for _, i in starts}
    total = longest = 0
    # Per connection, the time of the timeout that last counted and the
    # length of its run.
    runs: dict[tuple[str, str], tuple[int, int]] = {}
    with path.open() as handle:
        for row in csv.DictReader(handle):
            total += 1
            time = int(row["time_ns"])
            connection = row["rank"], row["dst_rank"]
            counted, run = runs.get(connection, (None, 0))
            if counted is None or time - counted >= 2 * A2A_RTO_NS:
                runs[connection] = time, 1
            elif time - counted >= A2A_RTO_NS:
                runs[connection] = time, run + 1
            longest = max(longest, runs[connection][1])
            if not starts:
                continue
            iteration = starts[0][1]
            for start, i in starts:
                if start > time:
                    break
                iteration = i
            per_iteration[iteration] += 1
    return {"rto_events": total,
            "rto_per_iteration": [per_iteration[i] for i in sorted(per_iteration)] if starts else None,
            "rto_run": longest}


def iteration_timing(directory: Path) -> dict[int, tuple[int, int]] | None:
    """{iteration: (start, completion)} of a run's completed iterations, the
    starts from summary.json or, for a summary that has none, from
    telemetry/collective_events.csv; None when the run is incomplete."""
    timing = json.loads((directory / "summary.json").read_text()).get("all_to_all") or {}
    if not timing.get("complete"):
        return None
    completions = {int(i): v for i, v in timing.get("iteration_completion_ns", {}).items()}
    starts = {int(i): v for i, v in (timing.get("iteration_start_ns") or {}).items()}
    events = directory / "telemetry" / "collective_events.csv"
    if not starts and events.exists():
        with events.open() as handle:
            for row in csv.DictReader(handle):
                if row["collective_type"] == "all_to_all":
                    step, start = int(row["training_step"]), int(row["start_time_ns"])
                    starts[step] = min(starts.get(step, start), start)
    return {i: (starts[i], completions[i]) for i in completions if i in starts}


def early_cost(directory: Path, healthy: Path | None, condition: str) -> float | None:
    """early_cost_ns (see the module docstring)."""
    if healthy is None or not (healthy / "summary.json").exists():
        return None
    reference = iteration_timing(healthy)
    if reference is None:
        return None
    timing = iteration_timing(directory)
    if timing is None:
        return float("inf")
    onset = fault_onset_ns(condition)
    window = [i for i, (start, _) in timing.items() if onset <= start <= onset + A2A_EARLY_WINDOW_NS]
    if not window or any(i not in reference for i in window):
        return None
    return sum(timing[i][1] - reference[i][1] for i in window)


def _run_roots(rows: list[dict], out_root: Path) -> dict[str, Path]:
    """Each run's out root by key: the out_root a row of another root
    carries (a2a_root_rows), else out_root."""
    return {row["key"]: row.get("out_root", out_root) for row in rows}


def a2a_root_rows(root: Path, stage: str) -> list[dict]:
    """One stage's runs in another tabulated out root (runs.json beside the
    run directories), as a2a_rows gives them, each carrying that root as
    out_root."""
    return [{**row, "out_root": root} for row in a2a_rows([root / "runs.json"], root, stage)]


def a2a_candidates(rows: list[dict], out_root: Path, reference: str | None = None) -> list[dict]:
    """a2a_cost_cells (verdicts against reference when given) with every
    arm's reaction_ns and early_cost_ns per seed and their means over the
    seeds that have one."""
    roots = _run_roots(rows, out_root)
    measured = {}
    for row in a2a_costs(rows):
        interval = (row.get("setting") or {}).get("loss_interval_ns", A2A_DEFAULT_LOSS_INTERVAL_NS)
        directory = roots[row["key"]] / row["key"]
        healthy = None if row["healthy_key"] is None else roots[row["healthy_key"]] / row["healthy_key"]
        measured[row["key"]] = {**reaction(directory, row["condition"], interval),
                                **receiver_reaction(directory, row["condition"]),
                                **rto_counts(directory),
                                "early_cost_ns": early_cost(directory, healthy, row["condition"])}
    cells = a2a_cost_cells(rows, reference)
    for cell in cells:
        for entry in cell["arms"].values():
            entry["rto_per_iteration"] = [measured[k]["rto_per_iteration"] for k in entry["keys"]]
            entry["rto_run"] = [measured[k]["rto_run"] for k in entry["keys"]]
            known = [v for v in entry["rto_run"] if v is not None]
            entry["rto_run_longest"] = max(known) if known else None
            entry["rto_run_over_limit"] = [seed for seed, v in zip(entry["seeds"], entry["rto_run"])
                                           if v is not None and v > A2A_RTO_RUN_LIMIT]
            for name in ("reaction_ns", "receiver_reaction_ns", "rto_events", "early_cost_ns"):
                values = [measured[k][name] for k in entry["keys"]]
                entry[name] = values
                known = [v for v in values if v is not None]
                entry[f"{name}_mean"] = sum(known) / len(known) if known else None
                entry[f"{name}_count"] = len(known)
            for basis in ("share_basis", "receiver_basis"):
                entry[basis] = next((measured[k][basis] for k in entry["keys"] if measured[k][basis]), None)
    return cells


def _count(value) -> str:
    return "-" if value is None else f"{value:,.1f}"


def _rto_run(entry: dict) -> str:
    """The arm's longest RTO run and the seeds whose run exceeds
    A2A_RTO_RUN_LIMIT."""
    if entry["rto_run_longest"] is None:
        return "-"
    over = entry["rto_run_over_limit"]
    return f"{entry['rto_run_longest']} ({', '.join(f's{seed}' for seed in over) or 'none'})"


def candidates_markdown(cells: list[dict]) -> str:
    lines = [
        "| group | size | condition | arm | cost mean us | tail cost mean us "
        "| reaction mean us (seeds) | receiver reaction mean us (seeds) "
        "| early cost mean us (seeds) | RTOs mean (seeds) "
        f"| longest RTO run (seeds over {A2A_RTO_RUN_LIMIT}) |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for cell in cells:
        for label, entry in cell["arms"].items():
            lines.append(
                f"| {cell['group']} | {cell['size']} | {cell['condition']} | {label} "
                f"| {_us(entry['cost_ns_mean'])} | {_us(entry['tail_cost_ns_mean'])} "
                f"| {_us(entry['reaction_ns_mean'])} ({entry['reaction_ns_count']}) "
                f"| {_us(entry['receiver_reaction_ns_mean'])} ({entry['receiver_reaction_ns_count']}) "
                f"| {_us(entry['early_cost_ns_mean'])} ({entry['early_cost_ns_count']}) "
                f"| {_count(entry['rto_events_mean'])} ({entry['rto_events_count']}) "
                f"| {_rto_run(entry)} |"
            )
    return "\n".join(lines) + "\n"


def flap_periods(flap: dict) -> list[tuple[str, int, int, int | None]]:
    """(kind, occurrence, start, end) of every on- and off-period of a
    flapping link, in time order; the last off-period has no end."""
    out = []
    for k in range(flap["repeat"]):
        start = flap["start_ns"] + k * flap["period_ns"]
        out.append(("on", k, start, start + flap["on_ns"]))
        last = k == flap["repeat"] - 1
        out.append(("off", k, start + flap["on_ns"], None if last else start + flap["period_ns"]))
    return out


def _inside(time: int, start: int, end: int | None) -> bool:
    return start <= time and (end is None or time < end)


def period_costs(directory: Path, healthy: Path | None, periods) -> list[dict]:
    """cost_ns and the iteration count of every period (see the module
    docstring)."""
    reference = None
    if healthy is not None and (healthy / "summary.json").exists():
        reference = iteration_timing(healthy)
    timing = iteration_timing(directory)
    out = []
    for _, _, start, end in periods:
        if timing is None:
            out.append({"cost_ns": None if reference is None else float("inf"), "iterations": None})
            continue
        inside = [i for i, (s, _) in timing.items() if _inside(s, start, end)]
        if not inside or reference is None or any(i not in reference for i in inside):
            out.append({"cost_ns": None, "iterations": len(inside)})
            continue
        out.append({"cost_ns": sum(timing[i][1] - reference[i][1] for i in inside) / len(inside),
                    "iterations": len(inside)})
    return out


CYCLE_MEASURES = {"on": ("detection_ns",),
                  "off": ("readmission_ns", "readmission_1ms_ns", "readmission_90pct_1ms_ns")}


def period_shares(directory: Path, condition: str, interval_ns: int, periods,
                  spine: int = DEGRADED_SPINE) -> dict:
    """Per period, the CYCLE_MEASURES of its kind (see the module
    docstring), and the rows' basis."""
    series = share_series(directory, condition, interval_ns, spine)
    out = []
    for kind, _, start, end in periods:
        values = dict.fromkeys(CYCLE_MEASURES[kind])
        if series is not None:
            totals, faulted, _ = series
            if kind == "on":
                values["detection_ns"] = first_below(totals, faulted, interval_ns, start, end)
            else:
                values["readmission_ns"] = first_above(totals, faulted, interval_ns, start, end)
                values["readmission_1ms_ns"] = first_window_above(
                    totals, faulted, interval_ns, start, end, A2A_SHARE_LIMIT)
                values["readmission_90pct_1ms_ns"] = first_window_above(
                    totals, faulted, interval_ns, start, end, A2A_FULL_SHARE)
        out.append(values)
    return {"periods": out, "share_basis": None if series is None else series[2]}


def a2a_cycles(rows: list[dict], out_root: Path) -> list[dict]:
    """Per (group, size, condition) cell with a flapping link, per arm and
    period: per-seed cost_ns, iteration counts and the CYCLE_MEASURES of the
    period's kind, and the means over the seeds that have one; per arm, the
    per-seed share_basis."""
    cells: dict[tuple, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for row in a2a_costs(rows):
        if "flap" not in row:
            continue
        periods = flap_periods(row["flap"])
        interval = (row.get("setting") or {}).get("loss_interval_ns", A2A_DEFAULT_LOSS_INTERVAL_NS)
        directory = out_root / row["key"]
        healthy = None if row["healthy_key"] is None else out_root / row["healthy_key"]
        costs = period_costs(directory, healthy, periods)
        shares = period_shares(directory, row["condition"], interval, periods)
        cells[(row["group"], row["size"], row["condition"])][row["arm"]].append(
            {"row": row, "periods": periods, "costs": costs, "shares": shares})

    def mean(values):
        known = [v for v in values if v is not None]
        return (sum(known) / len(known) if known else None), len(known)

    out = []
    for (group, size, condition), arms in sorted(cells.items()):
        table = {}
        for arm, runs in sorted(arms.items()):
            runs.sort(key=lambda r: r["row"]["seed"])
            periods = runs[0]["periods"]
            if any(r["periods"] != periods for r in runs):
                raise ValueError(f"{arm} on {group}-{size}-{condition} runs different flap layouts")
            entries = []
            for n, (kind, occurrence, start, end) in enumerate(periods):
                cost = [r["costs"][n]["cost_ns"] for r in runs]
                cost_mean, cost_count = mean(cost)
                entry = {
                    "kind": kind, "occurrence": occurrence, "start_ns": start, "end_ns": end,
                    "iterations": [r["costs"][n]["iterations"] for r in runs],
                    "cost_ns": cost, "cost_ns_mean": cost_mean, "cost_ns_count": cost_count,
                }
                for names in CYCLE_MEASURES.values():
                    for name in names:
                        entry[name] = entry[f"{name}_mean"] = entry[f"{name}_count"] = None
                for name in CYCLE_MEASURES[kind]:
                    values = [r["shares"]["periods"][n][name] for r in runs]
                    entry[name] = values
                    entry[f"{name}_mean"], entry[f"{name}_count"] = mean(values)
                entries.append(entry)
            table[arm] = {"keys": [r["row"]["key"] for r in runs], "seeds": [r["row"]["seed"] for r in runs],
                          "share_basis": [r["shares"]["share_basis"] for r in runs],
                          "periods": entries}
        out.append({"group": group, "size": size, "condition": condition, "arms": table})
    return out


def _ms(value) -> str:
    return "end" if value is None else f"{value / 1e6:,.3f}"


def cycles_markdown(cells: list[dict]) -> str:
    """A table per arm: a row per cell and period."""
    arms = sorted({arm for cell in cells for arm in cell["arms"]})
    lines = []
    for arm in arms:
        lines += [
            f"### {arm}", "",
            "| group | size | condition | share basis | period | start ms | end ms | iterations (seeds) "
            "| cost mean us (seeds) | detection mean us (seeds) | re-admission mean us (seeds) "
            "| re-admission 1 ms mean us (seeds) | 90 percent 1 ms mean us (seeds) |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for cell in cells:
            entry = cell["arms"].get(arm)
            if entry is None:
                continue
            basis = ", ".join(sorted({b for b in entry["share_basis"] if b})) or "-"
            for period in entry["periods"]:
                iterations = ", ".join("-" if v is None else str(v) for v in period["iterations"])
                measures = " | ".join(
                    "" if period[f"{name}_count"] is None else
                    f"{_us(period[f'{name}_mean'])} ({period[f'{name}_count']})"
                    for name in ("detection_ns", "readmission_ns", "readmission_1ms_ns", "readmission_90pct_1ms_ns"))
                lines.append(
                    f"| {cell['group']} | {cell['size']} | {cell['condition']} | {basis} "
                    f"| {period['kind']} {period['occurrence']} | {_ms(period['start_ns'])} | {_ms(period['end_ns'])} "
                    f"| {iterations} | {_us(period['cost_ns_mean'])} ({period['cost_ns_count']}) | {measures} |"
                )
        lines.append("")
    return "\n".join(lines)


# The compute gap of a (group, size) on a fixture from its healthy probe run.

A2A_COMPUTE_FRACTION = 2 / 3
# A decode layer exchange's period, dispatch to next dispatch.
A2A_LAYER_PERIOD_NS = 711_000


def compute_gap(median_ns: float, layer: bool) -> int:
    """The compute gap from a healthy median iteration completion (see the
    module docstring); layer for a decode layer exchange."""
    if layer:
        return math.ceil((A2A_LAYER_PERIOD_NS - median_ns) / 1000) * 1000
    return round(median_ns * A2A_COMPUTE_FRACTION / 1000) * 1000


def a2a_compute_gaps(runs_files, out_root: Path, stage: str) -> list[dict]:
    """Per run of the stage with a summary: its group, size and fixture, the
    compute gap it ran at, the iterations it completed of those it ran, the
    median iteration completion and the compute gap it gives (see the module
    docstring)."""
    out = []
    for run in _listed_runs(runs_files):
        path = out_root / run["key"] / "summary.json"
        if run["stage"] != stage or not path.exists():
            continue
        summary = json.loads(path.read_text())
        stats = iteration_stats(summary)
        median = stats["iteration_p50_ns"] if (summary.get("all_to_all") or {}).get("complete") else None
        out.append({"key": run["key"], "group": run["group"], "size": run["size"],
                    "fixture": run.get("fixture"),
                    "probe_compute_ns": run["compute_ns"], "iterations": run["iterations"],
                    "iterations_completed": stats.get("iterations_completed"), "median_iteration_ns": median,
                    "compute_ns": None if median is None else compute_gap(median, "combine_bytes" in run)})
    return out


def compute_gaps_markdown(rows: list[dict]) -> str:
    def value(v) -> str:
        return "-" if v is None else str(v)

    lines = ["| group | size | iterations completed | median iteration us | compute_ns |", "|---|---|---|---|---|"]
    for row in rows:
        lines.append(f"| {row['group']} | {row['size']} | {value(row['iterations_completed'])} of {row['iterations']} "
                     f"| {_us(row['median_iteration_ns'])} | {value(row['compute_ns'])} |")
    return "\n".join(lines) + "\n"


# The sweep: per cell, the loss evidence a host of leaf 0 expects and sees
# on spine 3, and the hold arms' recovered fraction against the oracle's.

A2A_SWEEP_POOLED = "reps_hold_auto_tail_ack"
# The hold arms read against the pooled one, per destination leaf and per
# flow (profiles.py A2A_HOLD_ARMS_4K).
A2A_SWEEP_HOLDS = ("reps_hold_leaf_tail_ack", "reps_flow_hold_tail_ack")
A2A_SWEEP_ORACLE = "reps_oracle_tail"
A2A_SWEEP_REPS = "reps_tail"
A2A_SWEEP_UNIFORM = "uniform_tail"
A2A_SWEEP_LOSS_RATES = {"loss1e3_up": 1e-3, "loss3e3_up": 3e-3, "loss1e2_up": 1e-2, "loss3e2_up": 3e-2}
# The generator's packet payload and the system layer's dataset splits when
# a run's fixture sets none.
A2A_DEFAULT_PAYLOAD_BYTES = 1000
A2A_DEFAULT_DATASET_SPLITS = 4


def spine_lost(directory: Path, condition: str, spine: int = DEGRADED_SPINE) -> int | None:
    """The losses the sender loss trace counts on the spine over
    faulted_share_rows' rows; None without a trace."""
    path = directory / "ns3" / "spine_losses.csv"
    if not path.exists():
        return None
    with path.open() as handle:
        reader = csv.DictReader(handle)
        keep, _ = faulted_share_rows(condition, reader.fieldnames or ())
        return sum(int(row["lost"]) for row in reader if int(row["spine"]) == spine and keep(row))


def _cost_range(entry: dict) -> float | None:
    costs = entry["cost_ns"]
    return None if not costs or None in costs else max(costs) - min(costs)


def _versus(arms: dict, a: str, b: str) -> str | None:
    """The seed-rule verdict of arm a against arm b on cost, on the same
    seeds; None when either is missing."""
    if a not in arms or b not in arms or arms[a]["seeds"] != arms[b]["seeds"]:
        return None
    return verdict(arms[a]["cost_ns"], arms[b]["cost_ns"], a, b)


def a2a_shared_rows(stage: str, shared_root: Path) -> list[dict]:
    """The runs a stage takes from the one profiles.py a2a_shared_4k names,
    on the cells and healthy pairs both read, from that stage's out root,
    as a2a_rows gives them."""
    import profiles

    source, cells = profiles.a2a_shared_4k(stage)
    if source is None:
        raise ValueError(f"{stage} runs every cell it reads")
    return [row for row in a2a_rows([shared_root / "runs.json"], shared_root, source)
            if (row["group"], row["size"], row["condition"]) in cells]


def a2a_sweep(rows: list[dict], out_root: Path, shared_root: Path | None = None) -> list[dict]:
    """a2a_cost_cells against uniform_tail, with each cell's expected
    evidence, the realized losses, R, the gaps and the control (see the
    module docstring); with shared_root, over the rows and the runs their
    stage takes from that out root (a2a_shared_rows)."""
    directories = {row["key"]: out_root / row["key"] for row in rows}
    if shared_root is not None:
        shared = [r for stage in sorted({row["stage"] for row in rows}) for r in a2a_shared_rows(stage, shared_root)]
        directories.update((row["key"], shared_root / row["key"]) for row in shared)
        rows = rows + shared
    lost = {}
    for row in rows:
        if row["condition"] != "healthy":
            count = spine_lost(directories[row["key"]], row["condition"])
            lost[row["key"]] = (None if count is None
                                else count / A2A_HOSTS_PER_LEAF_OF_TRACE / row["iterations"])
    shapes = {(r["group"], r["size"], _fixture_key(r)): r for r in rows}
    out = []
    for cell in a2a_cost_cells(rows, uniform=A2A_SWEEP_UNIFORM):
        shape = shapes[(cell["group"], cell["size"], _fixture_key(cell))]
        arms = cell["arms"]
        off_leaf = shape["k"] + 1 - A2A_HOSTS_PER_LEAF_OF_TRACE
        fixture = shape.get("fixture") or {}
        payload = fixture.get("payload_bytes", A2A_DEFAULT_PAYLOAD_BYTES)
        flows = fixture.get("dataset_splits", A2A_DEFAULT_DATASET_SPLITS)
        messages = [shape["message_bytes"]] + ([shape["combine_bytes"]] if "combine_bytes" in shape else [])
        n_s_pair = sum(flows * math.ceil(m / flows / payload) for m in messages) / SPINES
        n_s_flow = n_s_pair / (flows * len(messages))
        p = A2A_SWEEP_LOSS_RATES.get(cell["condition"])
        for entry in arms.values():
            entry["cost_ns_range"] = _cost_range(entry)
            realized = [lost[k] for k in entry["keys"]]
            entry["spine_losses_per_host_iteration"] = (None if None in realized
                                                        else sum(realized) / len(realized))
        oracle = (arms.get(A2A_SWEEP_ORACLE) or {}).get("recovered_mean")
        ceiling = {}
        for arm in (A2A_SWEEP_POOLED,) + A2A_SWEEP_HOLDS:
            recovered = (arms.get(arm) or {}).get("recovered_mean")
            ceiling[arm] = recovered / oracle if recovered is not None and oracle is not None and oracle > 0 else None
        pooled = arms.get(A2A_SWEEP_POOLED) or {}
        gaps = {}
        for arm in A2A_SWEEP_HOLDS:
            other = arms.get(arm) or {}
            gaps[arm] = {
                "recovered_gap": (None if pooled.get("recovered_mean") is None or other.get("recovered_mean") is None
                                  else pooled["recovered_mean"] - other["recovered_mean"]),
                "cost_gap_ns": (None if pooled.get("cost_ns_mean") is None or other.get("cost_ns_mean") is None
                                else other["cost_ns_mean"] - pooled["cost_ns_mean"]),
                "verdict": _versus(arms, A2A_SWEEP_POOLED, arm),
            }
        control = None
        if off_leaf == 0:
            labels = sorted(arms)
            control = {
                "within_range_of_zero": {
                    label: (None if arms[label]["cost_ns_mean"] is None or arms[label]["cost_ns_range"] is None
                            else abs(arms[label]["cost_ns_mean"]) <= arms[label]["cost_ns_range"])
                    for label in labels},
                "seed_rule": [v for i, a in enumerate(labels) for b in labels[i + 1:]
                              if (v := _versus(arms, a, b)) not in (None, "within seed noise")],
            }
        out.append({
            "group": cell["group"], "size": cell["size"], "condition": cell["condition"],
            "payload_bytes": payload, "flows_per_pair": flows * len(messages), "n_s_flow": n_s_flow,
            "n_s_pair": n_s_pair, "p": p,
            "m_flow": None if p is None else n_s_flow * p, "m_pair": None if p is None else n_s_pair * p,
            "host_evidence": None if p is None else n_s_pair * p * off_leaf,
            "arms": arms, "oracle_versus_reps": _versus(arms, A2A_SWEEP_ORACLE, A2A_SWEEP_REPS),
            "ceiling": ceiling, "gaps": gaps, "control": control,
        })
    return out


def _number(value, form: str = "g") -> str:
    return "-" if value is None else format(value, form)


def sweep_markdown(cells: list[dict]) -> str:
    """A row per cell (evidence, R and the gaps), a row per cell and arm
    (cost and recovered fraction), and the control cells."""
    holds = (A2A_SWEEP_POOLED,) + A2A_SWEEP_HOLDS

    def gap(entry):
        return (f"{_number(entry['recovered_gap'], '.3f')}, {_us(entry['cost_gap_ns'])}, "
                f"{entry['verdict'] or 'unreadable'}")

    lines = [
        "| group | size | condition | payload | n_s flow / pair | p | m flow / pair | host evidence "
        "| spine 3 losses per host iteration "
        f"({' / '.join(holds)}) | {A2A_SWEEP_ORACLE} vs {A2A_SWEEP_REPS} | R ({' / '.join(holds)}) "
        + "".join(f"| {A2A_SWEEP_POOLED} - {arm}: recovered, cost us, verdict " for arm in A2A_SWEEP_HOLDS) + "|",
        "|---" * 11 + "|---" * len(A2A_SWEEP_HOLDS) + "|",
    ]
    for cell in cells:
        realized = " / ".join(_number((cell["arms"].get(arm) or {}).get("spine_losses_per_host_iteration"), ".2f")
                              for arm in holds)
        ratios = " / ".join(_number(cell["ceiling"][arm], ".3f") for arm in holds)
        lines.append(
            f"| {cell['group']} | {cell['size']} | {cell['condition']} | {cell['payload_bytes']} "
            f"| {_number(cell['n_s_flow'])} / {_number(cell['n_s_pair'])} | {_number(cell['p'])} "
            f"| {_number(cell['m_flow'])} / {_number(cell['m_pair'])} | {_number(cell['host_evidence'])} | {realized} "
            f"| {cell['oracle_versus_reps'] or 'unreadable'} | {ratios} "
            + "".join(f"| {gap(cell['gaps'][arm])} " for arm in A2A_SWEEP_HOLDS) + "|"
        )
    lines += ["", "| group | size | condition | arm | cost mean us | seed range us | recovered mean |",
              "|---|---|---|---|---|---|---|"]
    for cell in cells:
        for label, entry in cell["arms"].items():
            lines.append(f"| {cell['group']} | {cell['size']} | {cell['condition']} | {label} "
                         f"| {_us(entry['cost_ns_mean'])} | {_us(entry['cost_ns_range'])} "
                         f"| {_number(entry['recovered_mean'], '.3f')} |")
    for cell in cells:
        if cell["control"] is None:
            continue
        lines += ["", f"Control {cell['group']} {cell['size']} {cell['condition']}: mean cost within its seed range "
                  "of zero: " + ", ".join(f"{label} {within}" for label, within in
                                          cell["control"]["within_range_of_zero"].items())
                  + "; seed-rule verdicts other than noise: " + ("; ".join(cell["control"]["seed_rule"]) or "none")]
    return "\n".join(lines) + "\n"


# The downlink stage: both ends' holds and the receiver's tail counts per
# spine, beside a2a_candidates' measures.

A2A_DOWNLINK_SENDER = "reps_hold_auto_tail_ack"
A2A_DOWNLINK_RECEIVER = "reps_hold_auto_tail_ack_rx"
A2A_DOWNLINK_REFERENCE = "reps_tail"
A2A_RECEIVER_TAIL_EVENTS = ("receiver_tail_missing", "receiver_tail_missing_late")


def receiver_tail_counts(directory: Path) -> dict:
    """receiver_tail_missing_by_spine and receiver_tail_missing_late_by_spine
    (see the module docstring); empty when the run counted none."""
    transport = json.loads((directory / "summary.json").read_text()).get("transport", {})
    counts: dict[str, dict[int, int]] = {event: {} for event in A2A_RECEIVER_TAIL_EVENTS}
    for name, value in transport.items():
        event, _, plane = name.rpartition(":")
        base, separator, spine = event.rpartition("_spine")
        if plane == "control" and separator and base in counts and spine.isdigit():
            counts[base][int(spine)] = value["count"]
    return {f"{event}_by_spine": dict(sorted(by_spine.items())) for event, by_spine in counts.items()}


def spine_holds(path: Path, held) -> dict | None:
    """Per spine of a hold trace, its hold intervals (rows for which held
    is true) and its lost column summed (see the module docstring); None
    without the file."""
    if not path.exists():
        return None
    holds: dict[int, int] = defaultdict(int)
    lost: dict[int, int] = defaultdict(int)
    holding: dict[tuple, bool] = {}
    with path.open() as handle:
        reader = csv.DictReader(handle)
        series = [column for column in reader.fieldnames or ()
                  if column not in ("time_ns", "sent", "arrivals", "lost", "grade", "hold")]
        for row in reader:
            spine = int(row["spine"])
            lost[spine] += int(row["lost"])
            key = tuple(row[c] for c in series)
            now = held(row)
            if now and not holding.get(key):
                holds[spine] += 1
            holding[key] = now
    return {"holds": dict(sorted(holds.items())), "lost": dict(sorted(lost.items()))}


def _off_spine(traced: dict | None, name: str, spine: int = DEGRADED_SPINE) -> int | None:
    return None if traced is None else sum(v for k, v in traced[name].items() if k != spine)


def a2a_downlink(rows: list[dict], out_root: Path) -> list[dict]:
    """a2a_candidates against reps_tail with the downlink measures (see the
    module docstring)."""
    measured = {}
    for row in a2a_costs(rows):
        directory = out_root / row["key"]
        measured[row["key"]] = {
            **receiver_tail_counts(directory),
            "receiver_holds": spine_holds(directory / "ns3" / "receiver_holds.csv", lambda r: r["hold"] != "none"),
            "sender_holds": spine_holds(directory / "ns3" / "spine_losses.csv", lambda r: r["grade"] == "0"),
        }
    cells = a2a_candidates(rows, out_root, A2A_DOWNLINK_REFERENCE)
    for cell in cells:
        for entry in cell["arms"].values():
            for name in ("receiver_tail_missing_by_spine", "receiver_tail_missing_late_by_spine",
                         "receiver_holds", "sender_holds"):
                entry[name] = [measured[k][name] for k in entry["keys"]]
            for side in ("receiver_holds", "sender_holds"):
                entry[f"false_{side}"] = {name: [_off_spine(measured[k][side], name) for k in entry["keys"]]
                                          for name in ("holds", "lost")}
        cell["receiver_versus_sender"] = _versus(cell["arms"], A2A_DOWNLINK_RECEIVER, A2A_DOWNLINK_SENDER)
    return cells


def _per_spine(dicts) -> str:
    """Per-spine counts summed over the seeds that have them, as
    spine:count; "-" when none has."""
    known = [d for d in dicts if d is not None]
    if not known:
        return "-"
    total: dict[int, int] = defaultdict(int)
    for d in known:
        for spine, count in d.items():
            total[spine] += count
    return ", ".join(f"{spine}:{count}" for spine, count in sorted(total.items())) or "none"


def downlink_markdown(cells: list[dict]) -> str:
    """A row per cell and arm, and each cell's receiver against sender
    verdict."""
    lines = [
        "| group | size | condition | arm | cost mean us (seeds) | vs reps_tail | reaction us | receiver reaction us "
        "| receiver tail missing | late | receiver holds | receiver lost | sender holds | sender lost "
        "| false receiver holds, lost | false sender holds, lost "
        f"| longest RTO run (seeds over {A2A_RTO_RUN_LIMIT}) |",
        "|---" * 17 + "|",
    ]
    for cell in cells:
        for label, entry in cell["arms"].items():
            def false(side):
                values = entry[f"false_{side}"]
                return "-" if None in values["holds"] else f"{sum(values['holds'])}, {sum(values['lost'])}"

            lines.append(
                f"| {cell['group']} | {cell['size']} | {cell['condition']} | {label} "
                f"| {_us(entry['cost_ns_mean'])} ({len(entry['seeds'])}) "
                f"| {entry.get('versus_reference_cost') or '-'} "
                f"| {_us(entry['reaction_ns_mean'])} | {_us(entry['receiver_reaction_ns_mean'])} "
                f"| {_per_spine(entry['receiver_tail_missing_by_spine'])} "
                f"| {_per_spine(entry['receiver_tail_missing_late_by_spine'])} "
                f"| {_per_spine([h and h['holds'] for h in entry['receiver_holds']])} "
                f"| {_per_spine([h and h['lost'] for h in entry['receiver_holds']])} "
                f"| {_per_spine([h and h['holds'] for h in entry['sender_holds']])} "
                f"| {_per_spine([h and h['lost'] for h in entry['sender_holds']])} "
                f"| {false('receiver_holds')} | {false('sender_holds')} | {_rto_run(entry)} |"
            )
    lines.append("")
    for cell in cells:
        lines.append(f"- {cell['group']} {cell['size']} {cell['condition']}: {A2A_DOWNLINK_RECEIVER} against "
                     f"{A2A_DOWNLINK_SENDER}: {cell['receiver_versus_sender'] or 'unreadable'}")
    return "\n".join(lines) + "\n"


# The answer-route stage: the sender holds a fault makes correct under each
# routing of the answers, and the answers lost on each spine.

A2A_ACKROUTE_REFERENCE = "reps_tail"
A2A_ACK_ROUTES = ("data_spine", "spray")
# The leaf of the faulted link of a condition's direction: leaf 0's uplink
# for _up, the downlink to leaf 1 for _down.
A2A_FAULT_LEAF = {"up": 0, "down": 1}
A2A_ANSWER_DROP_EVENTS = ("answer_drop_up", "answer_drop_down")
# Each arm against the arm it adds one key to, on cost.
A2A_ACKROUTE_VERSUS = (("reps_hold_auto_tail4_route_oracle", "reps_hold_auto_tail4"),
                       ("reps_oracle_tail_route_oracle", "reps_oracle_tail"),
                       ("reps_hold_auto_tail_ack", "reps_hold_auto_tail4"),
                       ("reps_hold_auto_tail_ack", "reps_hold_auto_tail_ack_data_spine"),
                       ("reps_oracle_tail_fwd", "reps_oracle_tail"),
                       ("reps_oracle_tail_fwd_route_oracle", "reps_oracle_tail_route_oracle"),
                       ("reps_oracle_tail_fwd_route_oracle", "reps_oracle_tail_fwd"),
                       # And the forward oracle with the answers' against the
                       # stage's control.
                       ("reps_oracle_tail_fwd_route_oracle", "reps_hold_auto_tail_ack_data_spine"))
# The stage whose runs --with-root reads.
A2A_ACKROUTE_WITH_STAGE = "a2a_ackroute_4k"


def correct_hold(ack_route: str, condition: str, sender_leaf: int, destination_leaf: int | None,
                 spine: int) -> bool:
    """Whether a sender on sender_leaf holding spine toward destination_leaf
    (None: toward every leaf) holds what the fault of condition degrades
    under ack_route (see the module docstring)."""
    if ack_route not in A2A_ACK_ROUTES:
        raise ValueError(f"ack_route must be one of {A2A_ACK_ROUTES}, not {ack_route!r}")
    if condition == "healthy" or host_link_condition(condition) or spine != DEGRADED_SPINE:
        return False
    direction = condition.rpartition("_")[2]
    if direction not in A2A_FAULT_LEAF:
        raise ValueError(f"condition {condition!r} names no faulted direction")
    leaf = A2A_FAULT_LEAF[direction]
    # The data crossing the link: up, a sender of the link's leaf toward any
    # leaf; down, any sender toward it.
    data = sender_leaf == leaf if direction == "up" else destination_leaf == leaf
    # Under data_spine the answers return over the data's spine: up, those
    # the link's leaf sends back to any sender; down, those sent back to a
    # sender of the link's leaf.
    answers = destination_leaf == leaf if direction == "up" else sender_leaf == leaf
    return data or (ack_route == "data_spine" and answers)


def sender_hold_reading(path: Path, condition: str, ack_route: str) -> dict | None:
    """correct_holds and false_holds, the sender hold intervals of the trace
    (grade 0) correct_hold takes for correct and the others, and
    correct_held_rows and false_held_rows, the rows that hold, each taken
    alike; None without the file (see the module docstring)."""
    if not path.exists():
        return None
    counts = {"correct_holds": 0, "false_holds": 0, "correct_held_rows": 0, "false_held_rows": 0}
    holding: dict[tuple, bool] = {}
    with path.open() as handle:
        reader = csv.DictReader(handle)
        series = [column for column in reader.fieldnames or ()
                  if column not in ("time_ns", "sent", "arrivals", "lost", "grade", "hold")]
        for row in reader:
            if "destination_leaf" in row:
                destination = int(row["destination_leaf"])
            elif "destination" in row:
                destination = int(row["destination"]) // A2A_HOSTS_PER_LEAF_OF_TRACE
            else:
                destination = None
            correct = correct_hold(ack_route, condition, int(row["rank"]) // A2A_HOSTS_PER_LEAF_OF_TRACE,
                                   destination, int(row["spine"]))
            kind = "correct" if correct else "false"
            key = tuple(row[c] for c in series)
            now = row["grade"] == "0"
            if now:
                counts[f"{kind}_held_rows"] += 1
                if not holding.get(key):
                    counts[f"{kind}_holds"] += 1
            holding[key] = now
    return counts


def answer_drops(directory: Path) -> dict:
    """answer_drop_up_by_spine and answer_drop_down_by_spine, from
    summary.json's transport rows <event>_spine<k>:control; empty when the
    run lost none."""
    transport = json.loads((directory / "summary.json").read_text()).get("transport", {})
    counts: dict[str, dict[int, int]] = {event: {} for event in A2A_ANSWER_DROP_EVENTS}
    for name, value in transport.items():
        event, _, plane = name.rpartition(":")
        base, separator, spine = event.rpartition("_spine")
        if plane == "control" and separator and base in counts and spine.isdigit():
            counts[base][int(spine)] = value["count"]
    return {f"{event}_by_spine": dict(sorted(by_spine.items())) for event, by_spine in counts.items()}


def a2a_ackroute(rows: list[dict], out_root: Path, ack_route: str | None = None) -> list[dict]:
    """a2a_candidates against reps_tail with the answer-route measures (see
    the module docstring); ack_route reads every run's holds under one
    routing, else each under its setting's ack_route (data_spine when
    unset)."""
    def routing(row):
        return ack_route or (row.get("setting") or {}).get("ack_route", "data_spine")

    by_key = {r["key"]: r for r in rows}
    roots = _run_roots(rows, out_root)
    measured = {}
    for row in a2a_costs(rows):
        directory = roots[row["key"]] / row["key"]
        healthy = by_key.get(row["healthy_key"])
        measured[row["key"]] = {
            **answer_drops(directory),
            "ack_route": routing(row),
            "sender_hold_reading": sender_hold_reading(directory / "ns3" / "spine_losses.csv", row["condition"],
                                                       routing(row)),
            "healthy_sender_hold_reading": None if healthy is None else sender_hold_reading(
                roots[healthy["key"]] / healthy["key"] / "ns3" / "spine_losses.csv", "healthy", routing(healthy)),
        }
    cells = a2a_candidates(rows, out_root, A2A_ACKROUTE_REFERENCE)
    for cell in cells:
        for entry in cell["arms"].values():
            for name in ("ack_route", "answer_drop_up_by_spine", "answer_drop_down_by_spine", "sender_hold_reading",
                         "healthy_sender_hold_reading"):
                entry[name] = [measured[k][name] for k in entry["keys"]]
        cell["versus"] = {f"{a} against {b}": _versus(cell["arms"], a, b) for a, b in A2A_ACKROUTE_VERSUS
                          if a in cell["arms"] and b in cell["arms"]}
    return cells


def _summed(readings, name: str) -> str:
    """name summed over the seeds' hold readings; "-" when none has one."""
    known = [r[name] for r in readings if r is not None]
    return "-" if not known else str(sum(known))


def ackroute_markdown(cells: list[dict]) -> str:
    """A row per cell and arm, and each cell's verdicts."""
    lines = [
        "| group | size | condition | arm | ack route | cost mean us (seeds) | vs reps_tail | reaction us "
        "| correct holds, rows | false holds, rows | healthy false holds, rows | answers lost up | answers lost down "
        f"| longest RTO run (seeds over {A2A_RTO_RUN_LIMIT}) |",
        "|---" * 14 + "|",
    ]
    for cell in cells:
        for label, entry in cell["arms"].items():
            readings, healthy = entry["sender_hold_reading"], entry["healthy_sender_hold_reading"]
            lines.append(
                f"| {cell['group']} | {cell['size']} | {cell['condition']} | {label} "
                f"| {', '.join(sorted(set(entry['ack_route'])))} "
                f"| {_us(entry['cost_ns_mean'])} ({len(entry['seeds'])}) "
                f"| {entry.get('versus_reference_cost') or '-'} | {_us(entry['reaction_ns_mean'])} "
                f"| {_summed(readings, 'correct_holds')}, {_summed(readings, 'correct_held_rows')} "
                f"| {_summed(readings, 'false_holds')}, {_summed(readings, 'false_held_rows')} "
                f"| {_summed(healthy, 'false_holds')}, {_summed(healthy, 'false_held_rows')} "
                f"| {_per_spine(entry['answer_drop_up_by_spine'])} "
                f"| {_per_spine(entry['answer_drop_down_by_spine'])} | {_rto_run(entry)} |"
            )
    lines.append("")
    for cell in cells:
        for name, value in cell["versus"].items():
            lines.append(f"- {cell['group']} {cell['size']} {cell['condition']}: {name}: {value or 'unreadable'}")
    return "\n".join(lines) + "\n"


# The no-ack stage: each loss-hold arm without ack_loss_rtts against its ack
# arm on the same cell.

# profiles.py's A2A_NOACK_ARMS and A2A_NOACK_SOURCES: each no-ack arm's ack
# arm, and the stages that run the ack arms.
A2A_NOACK_ARMS = {"reps_hold_auto_tail4": "reps_hold_auto_tail_ack", "reps_hold_leaf_tail4": "reps_hold_leaf_tail_ack",
                  "reps_flow_hold_tail4": "reps_flow_hold_tail_ack"}
A2A_NOACK_SOURCES = ("a2a_tail_ack_4k", "a2a_sweep_4k", "a2a_decode_4k")


def a2a_ack_rows(ack_roots) -> list[dict]:
    """The runs of the A2A_NOACK_SOURCES' ack arms in the tabulated out roots
    given (runs.json beside the run directories), as a2a_rows gives them."""
    rows = []
    for root in ack_roots or ():
        for stage in A2A_NOACK_SOURCES:
            rows += [row for row in a2a_rows([root / "runs.json"], root, stage)
                     if row["arm"] in A2A_NOACK_ARMS.values()]
    return rows


def a2a_noack(rows: list[dict], out_root: Path, ack_rows: list[dict] | None = None) -> list[dict]:
    """a2a_candidates against reps_tail (see the module docstring), with each
    no-ack arm's ack arm on the same cell from ack_rows: its per-seed and
    mean cost, the no-ack arm's mean cost less it, and the seed-rule
    verdict of the no-ack arm against it on cost."""
    cells = a2a_candidates(rows, out_root, "reps_tail")
    ack_cells = {(c["group"], c["size"], c["condition"]): c for c in a2a_cost_cells(ack_rows or [])}
    for cell in cells:
        theirs = ack_cells.get((cell["group"], cell["size"], cell["condition"]), {"arms": {}})["arms"]
        for label, entry in cell["arms"].items():
            ack = theirs.get(A2A_NOACK_ARMS.get(label, ""))
            entry["ack_arm"] = A2A_NOACK_ARMS.get(label)
            entry["ack_stages"] = None if ack is None else ack["stages"]
            entry["ack_cost_ns"] = None if ack is None else ack["cost_ns"]
            entry["ack_cost_ns_mean"] = None if ack is None else ack["cost_ns_mean"]
            entry["minus_ack_cost_ns"] = (None if ack is None or None in (entry["cost_ns_mean"], ack["cost_ns_mean"])
                                          else entry["cost_ns_mean"] - ack["cost_ns_mean"])
            entry["versus_ack_cost"] = (None if ack is None or ack["seeds"] != entry["seeds"]
                                        else verdict(entry["cost_ns"], ack["cost_ns"], label, entry["ack_arm"]))
    return cells


def noack_markdown(cells: list[dict]) -> str:
    """A row per cell and no-ack arm."""
    lines = [
        "| group | size | condition | arm | cost mean us (seeds) | ack arm | ack cost mean us | no-ack less ack us "
        f"| vs ack arm | reaction us | longest RTO run (seeds over {A2A_RTO_RUN_LIMIT}) |",
        "|---" * 11 + "|",
    ]
    for cell in cells:
        for label, entry in cell["arms"].items():
            if label not in A2A_NOACK_ARMS:
                continue
            lines.append(
                f"| {cell['group']} | {cell['size']} | {cell['condition']} | {label} "
                f"| {_us(entry['cost_ns_mean'])} ({len(entry['seeds'])}) | {entry['ack_arm']} "
                f"| {_us(entry['ack_cost_ns_mean'])} | {_us(entry['minus_ack_cost_ns'])} "
                f"| {entry['versus_ack_cost'] or '-'} | {_us(entry['reaction_ns_mean'])} | {_rto_run(entry)} |"
            )
    return "\n".join(lines) + "\n"


# The host-link stage: no spine is faulted, so every sender hold of a spine is
# false; where the holds and the losses fall.

A2A_HOSTLINK_REFERENCE = "reps_tail"
# profiles.py's A2A_HOST_PREFIX and A2A_FAULT_HOST: a host-link condition's
# name starts with the prefix, and its faulted link is that host's.
A2A_HOST_PREFIX = "host"
A2A_FAULT_HOST = 9
A2A_HOSTLINK_BUCKETS = ("toward_faulted_leaf", "toward_other_leaves", "toward_every_leaf")
# profiles.py's A2A_HOSTLINK_COVER_ARMS: each coverage-first arm of
# a2a_hostlink_s_4k and the a2a_hostlink_4k arm it adds the key to, against
# which it is read on cost.
A2A_HOSTLINK_COVER_ARMS = {f"{arm}_cover": arm for arm in ("reps_hold_auto_tail_ack", "reps_hold_leaf_tail_ack",
                                                            "reps_flow_hold_tail_ack")}
# The stage whose runs --with-root reads.
A2A_HOSTLINK_WITH_STAGE = "a2a_hostlink_4k"


def host_link_condition(condition: str) -> bool:
    """Whether condition faults one host's access link, no spine."""
    return condition.startswith(A2A_HOST_PREFIX)


def host_link_reading(path: Path, host: int = A2A_FAULT_HOST) -> dict | None:
    """The sender holds of ns3/spine_losses.csv under a fault of host's link
    and the losses toward host's leaf (see the module docstring); None
    without the file."""
    if not path.exists():
        return None
    faulted_leaf = host // A2A_HOSTS_PER_LEAF_OF_TRACE
    series: dict[tuple, dict[str, int]] = {}
    holding: dict[tuple, bool] = {}
    lost: dict[int, int] = defaultdict(int)
    lost_toward: dict[int, int] = defaultdict(int)
    with path.open() as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if "destination_leaf" in row:
                destination = int(row["destination_leaf"])
            elif "destination" in row:
                destination = int(row["destination"]) // A2A_HOSTS_PER_LEAF_OF_TRACE
            else:
                destination = None
            spine, count = int(row["spine"]), int(row["lost"])
            lost[spine] += count
            if destination == faulted_leaf:
                lost_toward[spine] += count
            key = (int(row["rank"]), row.get("destination", row.get("destination_leaf")), spine)
            now = row["grade"] == "0"
            if now:
                entry = series.setdefault(key, {"rank": key[0], "destination_leaf": destination, "spine": spine,
                                                "holds": 0, "held_rows": 0})
                entry["held_rows"] += 1
                entry["holds"] += not holding.get(key)
            holding[key] = now
    buckets = {name: {"series": 0, "holds": 0, "held_rows": 0} for name in A2A_HOSTLINK_BUCKETS}
    for entry in series.values():
        bucket = buckets["toward_every_leaf" if entry["destination_leaf"] is None
                         else "toward_faulted_leaf" if entry["destination_leaf"] == faulted_leaf
                         else "toward_other_leaves"]
        bucket["series"] += 1
        bucket["holds"] += entry["holds"]
        bucket["held_rows"] += entry["held_rows"]
    return {"faulted_leaf": faulted_leaf, "holding_series": sorted(series.values(), key=lambda e: (
                e["rank"], -1 if e["destination_leaf"] is None else e["destination_leaf"], e["spine"])),
            **buckets, "lost_by_spine": dict(sorted(lost.items())),
            "lost_toward_faulted_leaf_by_spine": dict(sorted(lost_toward.items()))}


def a2a_hostlink(rows: list[dict], out_root: Path) -> list[dict]:
    """a2a_candidates against reps_tail with the host-link measures (see the
    module docstring)."""
    by_key = {r["key"]: r for r in rows}
    roots = _run_roots(rows, out_root)
    measured = {}
    for row in a2a_costs(rows):
        directory = roots[row["key"]] / row["key"]
        healthy = by_key.get(row["healthy_key"])
        route = (row.get("setting") or {}).get("ack_route", "data_spine")
        measured[row["key"]] = {
            "sender_hold_reading": sender_hold_reading(directory / "ns3" / "spine_losses.csv", row["condition"],
                                                       route),
            "healthy_sender_hold_reading": None if healthy is None else sender_hold_reading(
                roots[healthy["key"]] / healthy["key"] / "ns3" / "spine_losses.csv", "healthy", route),
            "host_link": (host_link_reading(directory / "ns3" / "spine_losses.csv")
                          if host_link_condition(row["condition"]) else None),
            "answer_loss_covered": answer_losses(directory)["answer_loss_covered"],
        }
    cells = a2a_candidates(rows, out_root, A2A_HOSTLINK_REFERENCE)
    for cell in cells:
        for entry in cell["arms"].values():
            for name in ("sender_hold_reading", "healthy_sender_hold_reading", "host_link", "answer_loss_covered"):
                entry[name] = [measured[k][name] for k in entry["keys"]]
        cell["versus"] = {f"{a} against {b}": _versus(cell["arms"], a, b)
                          for a, b in A2A_HOSTLINK_COVER_ARMS.items() if a in cell["arms"] and b in cell["arms"]}
    return cells


def _bucket(readings, name: str) -> str:
    """A host-link reading's bucket summed over the seeds as holds, rows
    (series); "-" when no seed has a reading."""
    known = [r[name] for r in readings if r is not None]
    if not known:
        return "-"
    return (f"{sum(b['holds'] for b in known)}, {sum(b['held_rows'] for b in known)} "
            f"({sum(b['series'] for b in known)})")


def hostlink_markdown(cells: list[dict]) -> str:
    """A row per cell and arm, and each cell's verdicts."""
    lines = [
        "| group | size | condition | arm | cost mean us (seeds) | vs reps_tail | false holds, rows "
        "| toward the faulted leaf: holds, rows (series) | toward other leaves | toward every leaf "
        "| healthy false holds, rows | lost toward the faulted leaf | lost | answer losses covered "
        f"| longest RTO run (seeds over {A2A_RTO_RUN_LIMIT}) |",
        "|---" * 15 + "|",
    ]
    for cell in cells:
        for label, entry in cell["arms"].items():
            readings, host = entry["sender_hold_reading"], entry["host_link"]
            healthy = entry["healthy_sender_hold_reading"]
            lines.append(
                f"| {cell['group']} | {cell['size']} | {cell['condition']} | {label} "
                f"| {_us(entry['cost_ns_mean'])} ({len(entry['seeds'])}) "
                f"| {entry.get('versus_reference_cost') or '-'} "
                f"| {_summed(readings, 'false_holds')}, {_summed(readings, 'false_held_rows')} "
                + "".join(f"| {_bucket(host, name)} " for name in A2A_HOSTLINK_BUCKETS)
                + f"| {_summed(healthy, 'false_holds')}, {_summed(healthy, 'false_held_rows')} "
                f"| {_per_spine([h and h['lost_toward_faulted_leaf_by_spine'] for h in host])} "
                f"| {_per_spine([h and h['lost_by_spine'] for h in host])} "
                f"| {_counted(entry['answer_loss_covered'])} | {_rto_run(entry)} |"
            )
    lines.append("")
    for cell in cells:
        for name, value in cell["versus"].items():
            lines.append(f"- {cell['group']} {cell['size']} {cell['condition']}: {name}: {value or 'unreadable'}")
    return "\n".join(lines) + "\n"


# The reaction stage: how soon each end holds the faulted spine.

A2A_REACTION_MEASURES = ("sender_hold_ns", "sender_lost_before", "receiver_hold_ns", "receiver_lost_before",
                         "lead_ns", "lead_iterations")


def receiver_share_rows(condition: str):
    """The receiver hold trace's rows of the faulted link, and their basis
    (see receiver_reaction_ns in the module docstring)."""
    if condition == "structural":
        return (lambda row: True), "every row"
    if condition.endswith("_up"):
        return (lambda row: int(row["sender_leaf"]) == 0), "sender_leaf 0"
    return (lambda row: int(row["rank"]) // A2A_HOSTS_PER_LEAF_OF_TRACE == 1), "receivers on leaf 1"


def first_hold(directory: Path, condition: str, receiver: bool, spine: int = DEGRADED_SPINE) -> tuple:
    """The end's hold time from the onset and the losses its rows counted
    up to it (see the module docstring); (None, None) without a hold."""
    path = directory / "ns3" / ("receiver_holds.csv" if receiver else "spine_losses.csv")
    if not path.exists():
        return None, None
    onset = fault_onset_ns(condition)
    lost: dict[int, int] = defaultdict(int)
    first = None
    with path.open() as handle:
        reader = csv.DictReader(handle)
        keep, _ = (receiver_share_rows(condition) if receiver
                   else faulted_share_rows(condition, reader.fieldnames or ()))
        for row in reader:
            if int(row["spine"]) != spine or not keep(row):
                continue
            end = int(row["time_ns"])
            lost[end] += int(row["lost"])
            held = row["hold"] != "none" if receiver else row["grade"] == "0"
            if held and end > onset and (first is None or end < first):
                first = end
    if first is None:
        return None, None
    return first - onset, sum(count for end, count in lost.items() if end <= first)


def a2a_reaction(rows: list[dict], out_root: Path) -> list[dict]:
    """a2a_cost_cells with the A2A_REACTION_MEASURES per seed and their means,
    and each cell's receiver lead over the sender arm (see the module
    docstring)."""
    healthy = {r["key"]: r for r in rows if r["condition"] == "healthy"}
    measured = {}
    for row in a2a_costs(rows):
        directory = out_root / row["key"]
        sender = first_hold(directory, row["condition"], False)
        receiver = first_hold(directory, row["condition"], True)
        pair = healthy.get(row["healthy_key"])
        iteration = None if pair is None or pair["iterations_ns"] is None else statistics.fmean(pair["iterations_ns"])
        lead = None if None in (sender[0], receiver[0]) else sender[0] - receiver[0]
        measured[row["key"]] = {"sender_hold_ns": sender[0], "sender_lost_before": sender[1],
                                "receiver_hold_ns": receiver[0], "receiver_lost_before": receiver[1],
                                "lead_ns": lead, "iteration_ns": iteration,
                                "lead_iterations": None if None in (lead, iteration) else lead / iteration}
    cells = a2a_cost_cells(rows)
    for cell in cells:
        for entry in cell["arms"].values():
            for name in A2A_REACTION_MEASURES:
                entry[name] = [measured[k][name] for k in entry["keys"]]
                entry[f"{name}_mean"] = _mean(entry[name])
        rx, sender = cell["arms"].get(A2A_DOWNLINK_RECEIVER), cell["arms"].get(A2A_DOWNLINK_SENDER)
        cell["receiver_lead_ns"], cell["receiver_lead_iterations"] = [], []
        if rx is not None and sender is not None and rx["seeds"] == sender["seeds"]:
            for rx_key, sender_key in zip(rx["keys"], sender["keys"]):
                ahead, behind = measured[rx_key]["receiver_hold_ns"], measured[sender_key]["sender_hold_ns"]
                iteration = measured[sender_key]["iteration_ns"]
                lead = None if None in (ahead, behind) else behind - ahead
                cell["receiver_lead_ns"].append(lead)
                cell["receiver_lead_iterations"].append(None if None in (lead, iteration) else lead / iteration)
    return cells


def reaction_markdown(cells: list[dict]) -> str:
    """A row per cell and arm, and each cell's receiver lead over the sender
    arm."""
    def seeds(values, form):
        return ", ".join("-" if v is None else format(v, form) for v in values) or "-"

    def us(values):
        return seeds([None if v is None else v / 1000 for v in values], ".1f")

    lines = [
        "| group | size | condition | arm | cost mean us | sender hold us (seeds) | sender lost before "
        "| receiver hold us (seeds) | receiver lost before | lead us (seeds) | lead iterations (seeds) |",
        "|---" * 11 + "|",
    ]
    for cell in cells:
        for label, entry in cell["arms"].items():
            lines.append(
                f"| {cell['group']} | {cell['size']} | {cell['condition']} | {label} | {_us(entry['cost_ns_mean'])} "
                f"| {us(entry['sender_hold_ns'])} | {seeds(entry['sender_lost_before'], 'd')} "
                f"| {us(entry['receiver_hold_ns'])} | {seeds(entry['receiver_lost_before'], 'd')} "
                f"| {us(entry['lead_ns'])} | {seeds(entry['lead_iterations'], '.2f')} |"
            )
    lines.append("")
    for cell in cells:
        lines.append(f"- {cell['group']} {cell['size']} {cell['condition']}: {A2A_DOWNLINK_RECEIVER}'s receiver "
                     f"ahead of {A2A_DOWNLINK_SENDER}'s sender by {us(cell['receiver_lead_ns'])} us, "
                     f"{seeds(cell['receiver_lead_iterations'], '.2f')} iterations")
    return "\n".join(lines) + "\n"


# The downlink stage under spray: a2a_downlink's and a2a_reaction's readings
# of every arm, and both ends' correct and false holds.

# Each receiver arm against its sender arm, the receiver arm without
# ack_loss_rtts against the one with it, and the sender arm with
# ack_loss_rtts against the one without, on cost.
A2A_DOWNLINK_SPRAY_VERSUS = (("reps_hold_auto_tail_ack_rx_closed", "reps_hold_auto_tail_ack"),
                             ("reps_hold_auto_tail4_rx_closed", "reps_hold_auto_tail4"),
                             ("reps_hold_auto_tail4_rx_closed", "reps_hold_auto_tail_ack_rx_closed"),
                             ("reps_hold_auto_tail_ack", "reps_hold_auto_tail4"))
A2A_DOWNLINK_SPRAY_FIRST = ("sender_hold_ns", "sender_lost_before", "receiver_hold_ns", "receiver_lost_before")
A2A_DOWNLINK_SPRAY_READINGS = ("ack_route", "receiver_tail_missing_by_spine", "receiver_tail_missing_late_by_spine",
                               "sender_hold_reading", "receiver_hold_reading", "healthy_sender_hold_reading",
                               "healthy_receiver_hold_reading")


def correct_receiver_hold(condition: str, receiver_leaf: int, sender_leaf: int, spine: int) -> bool:
    """Whether a receiver on receiver_leaf holding spine for the senders of
    sender_leaf holds what the fault of condition degrades (see the module
    docstring)."""
    if (condition == "healthy" or host_link_condition(condition) or spine != DEGRADED_SPINE
            or sender_leaf == receiver_leaf):
        return False
    direction = condition.rpartition("_")[2]
    if direction not in A2A_FAULT_LEAF:
        raise ValueError(f"condition {condition!r} names no faulted direction")
    leaf = A2A_FAULT_LEAF[direction]
    return sender_leaf == leaf if direction == "up" else receiver_leaf == leaf


def receiver_hold_reading(path: Path, condition: str) -> dict | None:
    """correct_holds, false_holds, correct_held_rows and false_held_rows of
    the receiver hold trace (hold not none), as sender_hold_reading counts
    the sender's, each row taken by correct_receiver_hold; None without the
    file."""
    if not path.exists():
        return None
    counts = {"correct_holds": 0, "false_holds": 0, "correct_held_rows": 0, "false_held_rows": 0}
    holding: dict[tuple, bool] = {}
    with path.open() as handle:
        reader = csv.DictReader(handle)
        series = [column for column in reader.fieldnames or ()
                  if column not in ("time_ns", "arrivals", "lost", "hold")]
        for row in reader:
            correct = correct_receiver_hold(condition, int(row["rank"]) // A2A_HOSTS_PER_LEAF_OF_TRACE,
                                            int(row["sender_leaf"]), int(row["spine"]))
            kind = "correct" if correct else "false"
            key = tuple(row[c] for c in series)
            now = row["hold"] != "none"
            if now:
                counts[f"{kind}_held_rows"] += 1
                if not holding.get(key):
                    counts[f"{kind}_holds"] += 1
            holding[key] = now
    return counts


def a2a_downlink_spray(rows: list[dict], out_root: Path) -> list[dict]:
    """a2a_candidates against reps_tail with each end's first hold, the tail
    counts and both ends' hold readings (see the module docstring)."""
    by_key = {r["key"]: r for r in rows}
    measured = {}
    for row in a2a_costs(rows):
        directory = out_root / row["key"]
        healthy = by_key.get(row["healthy_key"])
        route = (row.get("setting") or {}).get("ack_route", "data_spine")
        sender = first_hold(directory, row["condition"], False)
        receiver = first_hold(directory, row["condition"], True)

        def readings(run_directory, condition):
            return (sender_hold_reading(run_directory / "ns3" / "spine_losses.csv", condition, route),
                    receiver_hold_reading(run_directory / "ns3" / "receiver_holds.csv", condition))

        faulted = readings(directory, row["condition"])
        paired = (None, None) if healthy is None else readings(out_root / healthy["key"], "healthy")
        measured[row["key"]] = {
            **receiver_tail_counts(directory), "ack_route": route,
            "sender_hold_ns": sender[0], "sender_lost_before": sender[1],
            "receiver_hold_ns": receiver[0], "receiver_lost_before": receiver[1],
            "sender_hold_reading": faulted[0], "receiver_hold_reading": faulted[1],
            "healthy_sender_hold_reading": paired[0], "healthy_receiver_hold_reading": paired[1],
        }
    cells = a2a_candidates(rows, out_root, A2A_DOWNLINK_REFERENCE)
    for cell in cells:
        for entry in cell["arms"].values():
            for name in A2A_DOWNLINK_SPRAY_FIRST + A2A_DOWNLINK_SPRAY_READINGS:
                entry[name] = [measured[k][name] for k in entry["keys"]]
            for name in A2A_DOWNLINK_SPRAY_FIRST:
                entry[f"{name}_mean"] = _mean(entry[name])
        cell["versus"] = {f"{a} against {b}": _versus(cell["arms"], a, b) for a, b in A2A_DOWNLINK_SPRAY_VERSUS
                          if a in cell["arms"] and b in cell["arms"]}
    return cells


def downlink_spray_markdown(cells: list[dict]) -> str:
    """A row per cell and arm, and each cell's verdicts."""
    def seeds(values, form):
        return ", ".join("-" if v is None else format(v, form) for v in values) or "-"

    def us(values):
        return seeds([None if v is None else v / 1000 for v in values], ".1f")

    def holds(readings, kind):
        return f"{_summed(readings, f'{kind}_holds')}, {_summed(readings, f'{kind}_held_rows')}"

    lines = [
        "| group | size | condition | arm | cost mean us (seeds) | vs reps_tail | sender hold us (seeds) "
        "| sender lost before | receiver hold us (seeds) | receiver lost before | receiver tail missing | late "
        "| sender correct holds, rows | sender false holds, rows | receiver correct holds, rows "
        "| receiver false holds, rows | healthy false holds, rows: sender; receiver "
        f"| longest RTO run (seeds over {A2A_RTO_RUN_LIMIT}) |",
        "|---" * 18 + "|",
    ]
    for cell in cells:
        for label, entry in cell["arms"].items():
            sender, receiver = entry["sender_hold_reading"], entry["receiver_hold_reading"]
            lines.append(
                f"| {cell['group']} | {cell['size']} | {cell['condition']} | {label} "
                f"| {_us(entry['cost_ns_mean'])} ({len(entry['seeds'])}) "
                f"| {entry.get('versus_reference_cost') or '-'} "
                f"| {us(entry['sender_hold_ns'])} | {seeds(entry['sender_lost_before'], 'd')} "
                f"| {us(entry['receiver_hold_ns'])} | {seeds(entry['receiver_lost_before'], 'd')} "
                f"| {_per_spine(entry['receiver_tail_missing_by_spine'])} "
                f"| {_per_spine(entry['receiver_tail_missing_late_by_spine'])} "
                f"| {holds(sender, 'correct')} | {holds(sender, 'false')} "
                f"| {holds(receiver, 'correct')} | {holds(receiver, 'false')} "
                f"| {holds(entry['healthy_sender_hold_reading'], 'false')}; "
                f"{holds(entry['healthy_receiver_hold_reading'], 'false')} | {_rto_run(entry)} |"
            )
    lines.append("")
    for cell in cells:
        for name, value in cell["versus"].items():
            lines.append(f"- {cell['group']} {cell['size']} {cell['condition']}: {name}: {value or 'unreadable'}")
    return "\n".join(lines) + "\n"


# The two-end stage: the sender's coverage first and the answers' path
# sequences, every answer but the control's on a spine its receiver draws.

A2A_TWOEND_REFERENCE = "reps_hold_auto_tail4"
# The coverage-first arm against the arm without it, the arm with the
# answers' path sequences against the coverage-first arm, the same with
# ack_route_oracle and the stage's control, and a2a_twoend_base_4k's
# per-connection routes against the coverage-first arm and the arm with the
# answers' path sequences against them; a2a_twoend_retract_4k's arm against
# the coverage-first arm and a2a_twoend_4k's reps_hold_auto_tail4_cover_dup,
# and the arm with the answers' path sequences against it, on cost.
A2A_TWOEND_VERSUS = (("reps_hold_auto_tail4_cover", "reps_hold_auto_tail4"),
                     ("reps_hold_auto_tail4_cover_answer_seq", "reps_hold_auto_tail4_cover"),
                     ("reps_hold_auto_tail4_cover_answer_seq", "reps_hold_auto_tail4_cover_route_oracle"),
                     ("reps_hold_auto_tail4_cover_answer_seq", "reps_hold_auto_tail_ack_data_spine"),
                     ("reps_hold_auto_tail4_cover_dup", "reps_hold_auto_tail4_cover"),
                     ("reps_hold_auto_tail4_cover_answer_seq", "reps_hold_auto_tail4_cover_dup"),
                     ("reps_hold_auto_tail4_cover_reverse_set", "reps_hold_auto_tail4_cover"),
                     ("reps_hold_auto_tail4_cover_repath", "reps_hold_auto_tail4_cover"),
                     ("reps_hold_auto_tail4_cover_answer_seq", "reps_hold_auto_tail4_cover_reverse_set"),
                     ("reps_hold_auto_tail4_cover_answer_seq", "reps_hold_auto_tail4_cover_repath"),
                     ("reps_hold_auto_tail4_cover_retract", "reps_hold_auto_tail4_cover"),
                     ("reps_hold_auto_tail4_cover_answer_seq", "reps_hold_auto_tail4_cover_retract"),
                     ("reps_hold_auto_tail4_cover_retract", "reps_hold_auto_tail4_cover_dup"))
A2A_ANSWER_LOSS_EVENTS = ("answer_loss_covered", "answer_route_lost", "answer_probe", "answer_route_hold",
                          "answer_route_release", "answer_set_probe", "answer_set_replace", "answer_repath",
                          "answer_loss_duplicate", "timeout_loss_data", "timeout_retract_release")
A2A_TWOEND_READINGS = (*A2A_ANSWER_LOSS_EVENTS, "answer_route_lost_by_spine", "sender_hold_reading",
                       "healthy_sender_hold_reading", "answer_hold_reading", "healthy_answer_hold_reading")
# The stage whose runs --with-root reads.
A2A_TWOEND_WITH_STAGE = "a2a_twoend_4k"


def answer_losses(directory: Path) -> dict:
    """The count of each of A2A_ANSWER_LOSS_EVENTS, from summary.json's
    transport rows <event>:control (None without the row: the run's keys
    are off), and answer_route_lost_by_spine, from the rows
    answer_route_lost_spine<k>:control."""
    transport = json.loads((directory / "summary.json").read_text()).get("transport", {})
    counts = {event: transport[f"{event}:control"]["count"] if f"{event}:control" in transport else None
              for event in A2A_ANSWER_LOSS_EVENTS}
    by_spine: dict[int, int] = {}
    for name, value in transport.items():
        event, _, plane = name.rpartition(":")
        base, separator, spine = event.rpartition("_spine")
        if plane == "control" and separator and base == "answer_route_lost" and spine.isdigit():
            by_spine[int(spine)] = value["count"]
    return {**counts, "answer_route_lost_by_spine": dict(sorted(by_spine.items()))}


def correct_answer_hold(condition: str, answering_leaf: int, answered_leaf: int, spine: int) -> bool:
    """Whether the answers from answering_leaf (their data's receivers) to
    answered_leaf (its senders) over spine cross the faulted link of
    condition: correct_receiver_hold with the answers' ends for the data's."""
    return correct_receiver_hold(condition, answered_leaf, answering_leaf, spine)


def answer_hold_reading(path: Path, condition: str) -> dict | None:
    """The answer holds of ns3/answer_holds.csv (see the module docstring);
    None without the file."""
    if not path.exists():
        return None
    hosts = A2A_HOSTS_PER_LEAF_OF_TRACE
    onset = fault_onset_ns(condition)
    sides = {side: {"correct_holds": 0, "false_holds": 0, "releases": 0, "held_at_end": 0}
             for side in ("sender", "receiver")}
    holding: dict[tuple, bool] = {}
    toward: dict[tuple[int, int], dict] = {}
    with path.open() as handle:
        for row in csv.DictReader(handle):
            side, rank, leaf, spine = row["side"], int(row["rank"]), int(row["leaf"]), int(row["spine"])
            held = row["held"] == "1"
            key = (side, rank, leaf, spine)
            if holding.get(key, False) == held:
                continue
            holding[key] = held
            # A sender's row holds a route of the answers from leaf to it, a
            # receiver's a spine of its answers to leaf.
            answering, answered = (leaf, rank // hosts) if side == "sender" else (rank // hosts, leaf)
            correct = correct_answer_hold(condition, answering, answered, spine)
            if held:
                sides[side][f"{'correct' if correct else 'false'}_holds"] += 1
            else:
                sides[side]["releases"] += 1
            if side != "receiver":
                continue
            entry = toward.setdefault((leaf, spine), {"sender_leaf": leaf, "spine": spine, "correct": correct,
                                                      "receivers": set(), "first_ns": None, "holds": 0,
                                                      "releases": 0})
            if held:
                entry["receivers"].add(rank)
                entry["holds"] += 1
                time = int(row["time_ns"]) - onset
                entry["first_ns"] = time if entry["first_ns"] is None else min(entry["first_ns"], time)
            else:
                entry["releases"] += 1
    for (side, *_), held in holding.items():
        sides[side]["held_at_end"] += held
    receiver_holds = []
    for entry in sorted(toward.values(), key=lambda e: (e["sender_leaf"], e["spine"])):
        receiver_holds.append({**entry, "receivers": len(entry["receivers"]),
                               "held_at_end": sum(held for (side, _, leaf, spine), held in holding.items()
                                                  if side == "receiver" and (leaf, spine)
                                                  == (entry["sender_leaf"], entry["spine"]))})
    return {"sender": sides["sender"], "receiver": sides["receiver"], "receiver_holds": receiver_holds}


def a2a_twoend(rows: list[dict], out_root: Path) -> list[dict]:
    """a2a_candidates against reps_hold_auto_tail4 with the answer losses,
    the sender holds read under spray and the answer holds (see the module
    docstring)."""
    by_key = {r["key"]: r for r in rows}
    roots = _run_roots(rows, out_root)
    measured = {}
    for row in a2a_costs(rows):
        directory = roots[row["key"]] / row["key"]
        healthy = by_key.get(row["healthy_key"])
        paired = None if healthy is None else roots[healthy["key"]] / healthy["key"]
        measured[row["key"]] = {
            **answer_losses(directory),
            "sender_hold_reading": sender_hold_reading(directory / "ns3" / "spine_losses.csv", row["condition"],
                                                       "spray"),
            "healthy_sender_hold_reading": None if paired is None else sender_hold_reading(
                paired / "ns3" / "spine_losses.csv", "healthy", "spray"),
            "answer_hold_reading": answer_hold_reading(directory / "ns3" / "answer_holds.csv", row["condition"]),
            "healthy_answer_hold_reading": None if paired is None else answer_hold_reading(
                paired / "ns3" / "answer_holds.csv", "healthy"),
        }
    cells = a2a_candidates(rows, out_root, A2A_TWOEND_REFERENCE)
    for cell in cells:
        for entry in cell["arms"].values():
            for name in A2A_TWOEND_READINGS:
                entry[name] = [measured[k][name] for k in entry["keys"]]
        cell["versus"] = {f"{a} against {b}": _versus(cell["arms"], a, b) for a, b in A2A_TWOEND_VERSUS
                          if a in cell["arms"] and b in cell["arms"]}
    return cells


def _counted(values) -> str:
    """values summed over the seeds that have one; "-" when none has."""
    known = [v for v in values if v is not None]
    return "-" if not known else str(sum(known))


def _side_summed(readings, side: str, name: str) -> str:
    """name of one side of the answer hold readings summed over the seeds;
    "-" when none has one."""
    return _summed([None if r is None else r[side] for r in readings], name)


def twoend_markdown(cells: list[dict]) -> str:
    """A row per cell and arm, the receivers' answer holds per seed, and
    each cell's verdicts."""
    def holds(readings, kind):
        return f"{_summed(readings, f'{kind}_holds')}, {_summed(readings, f'{kind}_held_rows')}"

    def answer_holds(readings, side):
        return (f"{_side_summed(readings, side, 'correct_holds')}, {_side_summed(readings, side, 'false_holds')}, "
                f"{_side_summed(readings, side, 'releases')}, {_side_summed(readings, side, 'held_at_end')}")

    lines = [
        f"| group | size | condition | arm | cost mean us (seeds) | vs {A2A_TWOEND_REFERENCE} | reaction us "
        "| sender correct holds, rows | sender false holds, rows | healthy false holds, rows "
        "| answer losses covered | answer route lost | by route spine "
        "| sender route holds: correct, false, released, at end "
        "| receiver answer holds: correct, false, released, at end | probe answers "
        "| set probes, replaced | repaths | timed out: duplicate answer losses, data losses "
        f"| longest RTO run (seeds over {A2A_RTO_RUN_LIMIT}) |",
        "|---" * 20 + "|",
    ]
    for cell in cells:
        for label, entry in cell["arms"].items():
            readings, answer = entry["sender_hold_reading"], entry["answer_hold_reading"]
            # A run without answer path sequences counts no route's.
            by_spine = [None if lost is None else spines
                        for lost, spines in zip(entry["answer_route_lost"], entry["answer_route_lost_by_spine"])]
            lines.append(
                f"| {cell['group']} | {cell['size']} | {cell['condition']} | {label} "
                f"| {_us(entry['cost_ns_mean'])} ({len(entry['seeds'])}) "
                f"| {entry.get('versus_reference_cost') or '-'} | {_us(entry['reaction_ns_mean'])} "
                f"| {holds(readings, 'correct')} | {holds(readings, 'false')} "
                f"| {holds(entry['healthy_sender_hold_reading'], 'false')} "
                f"| {_counted(entry['answer_loss_covered'])} | {_counted(entry['answer_route_lost'])} "
                f"| {_per_spine(by_spine)} "
                f"| {answer_holds(answer, 'sender')} | {answer_holds(answer, 'receiver')} "
                f"| {_counted(entry['answer_probe'])} "
                f"| {_counted(entry['answer_set_probe'])}, {_counted(entry['answer_set_replace'])} "
                f"| {_counted(entry['answer_repath'])} "
                f"| {_counted(entry['answer_loss_duplicate'])}, {_counted(entry['timeout_loss_data'])} "
                f"| {_rto_run(entry)} |"
            )
    lines.append("")
    for cell in cells:
        for label, entry in cell["arms"].items():
            for seed, reading in zip(entry["seeds"], entry["answer_hold_reading"]):
                if reading is None or not reading["receiver_holds"]:
                    continue
                spans = "; ".join(
                    f"leaf {h['sender_leaf']} spine {h['spine']} ({'correct' if h['correct'] else 'false'}): "
                    f"{h['receivers']} receivers from {_us(h['first_ns'])} us, {h['holds']} holds, "
                    f"{h['releases']} released, {h['held_at_end']} at end"
                    for h in reading["receiver_holds"])
                lines.append(f"- {cell['group']} {cell['size']} {cell['condition']} {label} s{seed}: "
                             f"receiver answer holds: {spans}")
    for cell in cells:
        for name, value in cell["versus"].items():
            lines.append(f"- {cell['group']} {cell['size']} {cell['condition']}: {name}: {value or 'unreadable'}")
    return "\n".join(lines) + "\n"


# Phase 9 calibration: thresholds per scope and (group, size) from the
# healthy runs of a2a_oracle_calibration, a2a_calibration,
# a2a_calibration_4k, a2a_decode_calibration_4k and
# ar_parity_calibration_4k.

A2A_CALIBRATION_STAGES = ("a2a_oracle_calibration", "a2a_calibration", "a2a_calibration_4k",
                          "a2a_decode_calibration_4k", "ar_parity_calibration_4k")
A2A_SCOPE_OF_ARM = {
    "graded_switch_queue": "switch_queue",
    "graded_per_connection": "per_connection",
    "graded_per_receiver_mix": "per_receiver",
}
A2A_LOSS_INTERVALS_NS = tuple(n * BASE_RTT_NS for n in (2, 10, 40))
A2A_HEADLINE_CELL = "ep64-2m"
A2A_HEADLINE_RATE = 1e-3
A2A_HOSTS_PER_LEAF = 8


def one_loss_thresholds(one_loss: float) -> list[float]:
    """(f/4, f/2, f) for the one-loss fraction f, rounded down to 6 decimals
    (and kept above 0) so that one loss at the median send count reaches the
    last and grades 0."""
    return [max(int(x * 1e6 + 1e-6) / 1e6, 1e-6) for x in (one_loss / 4, one_loss / 2, one_loss)]


def loss_thresholds(first_p99: float, sent_counts: list[int], count_floor: bool = True) -> dict:
    """The lost fraction's thresholds. Without count_floor, (t, 2t, 4t) for
    t the healthy 99th percentile, floored by scaled_thresholds at 1e-4.
    With it, f is the fraction one loss reads at the median per-spine send
    count; a p99 at or above f keeps the p99's (t, 2t, 4t), but a p99 below
    it (healthy loss on this fabric is exactly zero) gives (f/4, f/2, f):
    one loss per interval is the anomaly and grades 0. The x2/x4 shape
    belongs to the marked and trimmed fractions, whose healthy values are
    nonzero. "floored" flags the second case, "capped" the cap of
    scaled_thresholds; "rule" records which rule applied."""
    median_sent = statistics.median(sent_counts) if sent_counts else None
    one_loss = 1 / median_sent if count_floor and median_sent else 0.0
    if one_loss and first_p99 < one_loss:
        capped, values = False, one_loss_thresholds(one_loss)
        rule = "p99 zero: one loss grades 0" if first_p99 == 0 else "p99 below one loss: one loss grades 0"
    else:
        capped, values = scaled_thresholds(first_p99)
        rule = "p99, count floor" if count_floor else "p99, 1e-4 floor, no count floor"
    return {"p99": first_p99, "median_sent": median_sent, "rule": rule,
            "one_loss_fraction": one_loss if count_floor else None,
            "floored": bool(one_loss) and first_p99 < one_loss if count_floor else values[0] > first_p99,
            "capped": capped, "loss_thresholds": values}


def interval_losses(run: Path, interval_ns: int) -> dict[tuple[int, int, int], list[int]]:
    """The sender loss trace (two-base-RTT rows of time_ns, rank, spine, sent,
    lost) summed into intervals of interval_ns, aligned at 0 as the pool's
    are: {(rank, interval index, spine): [sent, lost]}, where a row ending
    at time t belongs to the interval ending at or after it."""
    sums: dict[tuple[int, int, int], list[int]] = defaultdict(lambda: [0, 0])
    with (run / "ns3" / "spine_losses.csv").open() as handle:
        for row in csv.DictReader(handle):
            cell = sums[(int(row["rank"]), (int(row["time_ns"]) - 1) // interval_ns, int(row["spine"]))]
            cell[0] += int(row["sent"])
            cell[1] += int(row["lost"])
    return sums


def trace_loss_thresholds(runs: list[Path]) -> dict[str, dict]:
    """Per candidate interval, loss_thresholds from the healthy lost fraction
    of every (host, interval, spine) with a send."""
    out = {}
    for interval in A2A_LOSS_INTERVALS_NS:
        fractions, sent = [], []
        for run in runs:
            for n, lost in interval_losses(run, interval).values():
                if n:
                    fractions.append(lost / n)
                    sent.append(n)
        out[str(interval)] = loss_thresholds(percentile(fractions, 0.99), sent)
    return out


def report_loss_thresholds(runs: list[Path]) -> dict:
    """The oracle's loss_thresholds, from its ground-truth loss fraction per
    report and spine with arrivals, by the rule of its congestion thresholds:
    the healthy 99th percentile, floored at 1e-4 only. The fraction is a
    probability read from switch state, not a count, so no count floor: one
    over the arrivals would blind the oracle to losses of 1e-2 and 1e-3."""
    fractions, arrivals = [], []
    for run in runs:
        for rows in reports(run).values():
            for r in rows:
                if int(r["arrivals"]):
                    fractions.append(float(r["loss_fraction"]))
                    arrivals.append(int(r["arrivals"]))
    return loss_thresholds(percentile(fractions, 0.99), arrivals, count_floor=False)


def headline(runs: list[Path], spine: int = DEGRADED_SPINE) -> dict:
    """P17: the expected losses at 1e-3 per pool interval of the longest
    candidate on the given spine for a leaf 0 sender, the mean send count
    over the (host, interval) pairs with a send there, times 1e-3. At least
    1 keeps the 1e-3 uplink cell as the headline; otherwise the 1e-2 one."""
    interval = max(A2A_LOSS_INTERVALS_NS)
    sends = [
        n for run in runs for (rank, _, s), (n, _) in interval_losses(run, interval).items()
        if s == spine and rank // A2A_HOSTS_PER_LEAF == 0 and n
    ]
    mean = statistics.fmean(sends) if sends else None
    expected = None if mean is None else mean * A2A_HEADLINE_RATE
    return {"interval_ns": interval, "intervals": len(sends), "mean_sent": mean,
            "expected_losses_at_1e3": expected,
            "condition": "loss1e3_up" if expected is not None and expected >= 1 else "loss1e2_up"}


def a2a_calibrate(runs_files, out_root: Path, existing: dict) -> dict:
    """a2a_calibration.json from the healthy calibration runs that have a
    summary, merged into existing: per scope and (group, size), named
    <group>-<size>-p<payload> for runs on a fixture with a packet payload, the Phase 8
    congestion (and, but for switch_queue, trim) thresholds of thresholds(),
    and loss_thresholds. The pooled pass's are a map from each candidate
    interval to its list, from its sender loss trace; per_connection takes
    the same map from the pooled pass of its cell (its own runs write no
    trace); switch_queue's are one list from its ground-truth loss fraction.
    "headline" (<group>-<size>-<condition>) is set by the P17 rule from the
    pooled EP64 2 MiB trace, unless existing holds a "headline_reason" for a
    headline set by hand; "headline_rule" is recorded either way."""
    groups: dict[tuple[str, str], list[Path]] = defaultdict(list)
    for run in _listed_runs(runs_files):
        directory = out_root / run["key"]
        if (run["stage"] in A2A_CALIBRATION_STAGES and run["condition"] == "healthy"
                and run["arm"] in A2A_SCOPE_OF_ARM and (directory / "summary.json").exists()):
            payload = (run.get("fixture") or {}).get("payload_bytes")
            cell = f"{run['group']}-{run['size']}" + ("" if payload is None else f"-p{payload}")
            groups[(A2A_SCOPE_OF_ARM[run["arm"]], cell)].append(directory)
    result = {key: (dict(value) if isinstance(value, dict) else value) for key, value in existing.items()}
    traced = {cell: [d for d in dirs if (d / "ns3" / "spine_losses.csv").exists()]
              for (scope, cell), dirs in groups.items() if scope == "per_receiver"}
    for (scope, cell), directories in sorted(groups.items()):
        switch = scope == "switch_queue"
        entry = thresholds(directories, ("mark_fraction", "loss_fraction") if switch else ("mark_fraction", "trim_fraction"))
        entry["runs"] = sorted(d.name for d in directories)
        if switch:
            loss = report_loss_thresholds(directories)
            entry["loss_thresholds"] = loss.pop("loss_thresholds")
            entry["loss_calibration"] = loss
        elif traced.get(cell):
            losses = trace_loss_thresholds(traced[cell])
            entry["loss_thresholds"] = {k: v.pop("loss_thresholds") for k, v in losses.items()}
            entry["loss_calibration"] = losses
            entry["loss_runs"] = sorted(d.name for d in traced[cell])
        result.setdefault(scope, {})[cell] = entry
    if traced.get(A2A_HEADLINE_CELL):
        decision = headline(traced[A2A_HEADLINE_CELL])
        if "headline_reason" not in result:
            result["headline"] = f"{A2A_HEADLINE_CELL}-{decision['condition']}"
        result["headline_rule"] = decision
    return result


def gate_line(row: dict) -> str:
    def us(values):
        if values is None:
            return "-"
        return ", ".join("-" if v is None else f"{v / 1000:,.1f}" for v in values)

    verdict_text = {None: "unreadable", True: "admitted", False: "dropped (non-discriminating)"}[row["admitted"]]
    ratio = "-" if row["oracle_over_uniform"] is None else f"{row['oracle_over_uniform']:.2f}"
    return (f"{row['group']} {row['size']} {row['condition']}: uniform cost us [{us(row['uniform_cost_ns'])}], "
            f"oracle [{us(row['oracle_cost_ns'])}], oracle/uniform mean {ratio}, "
            f"half rule {row['half_rule']}, seed rule {row['seed_rule']}: {verdict_text}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    t = sub.add_parser("thresholds")
    t.add_argument("runs", type=Path, nargs="+")
    t.add_argument("--switch-queue", action="store_true")
    g = sub.add_parser("gain")
    g.add_argument("runs", type=Path, nargs="+")
    b = sub.add_parser("binding")
    b.add_argument("runs", type=Path, nargs="+")
    c = sub.add_parser("compare")
    c.add_argument("--runs", type=Path, required=True)
    c.add_argument("--out-root", type=Path, required=True)
    c.add_argument("--stage")
    v = sub.add_parser("versus")
    v.add_argument("arm_a")
    v.add_argument("arm_b")
    v.add_argument("--runs", type=Path, required=True)
    v.add_argument("--out-root", type=Path, required=True)
    i = sub.add_parser("identity")
    i.add_argument("--out-root", type=Path, required=True)
    i.add_argument("--other-root", type=Path, required=True)
    i.add_argument("--runs", type=Path)
    for name in ("calibrate", "gains", "select", "cost", "a2a_gate", "a2a_calibrate", "a2a_gate_reps",
                 "a2a_candidates", "a2a_cycles", "a2a_compute_gaps", "a2a_sweep", "a2a_downlink",
                 "a2a_reaction", "a2a_ackroute", "a2a_noack", "a2a_hostlink", "a2a_downlink_spray", "a2a_twoend"):
        x = sub.add_parser(name)
        x.add_argument("--runs", type=Path, required=True,
                       nargs="+" if name in ("cost", "a2a_gate", "a2a_calibrate", "a2a_gate_reps", "a2a_candidates",
                                             "a2a_cycles", "a2a_compute_gaps", "a2a_sweep", "a2a_downlink",
                                             "a2a_reaction", "a2a_ackroute", "a2a_noack", "a2a_hostlink",
                                             "a2a_downlink_spray", "a2a_twoend")
                       else None)
        x.add_argument("--out-root", type=Path, required=True)
        if name in ("calibrate", "gains"):
            x.add_argument("--into", type=Path)
        if name in ("calibrate", "select", "cost", "a2a_gate"):
            x.add_argument("--stage")
        if name == "cost":
            x.add_argument("--reference", required=True)
        if name == "a2a_gate":
            x.add_argument("--json", action="store_true")
        if name in ("a2a_gate_reps", "a2a_compute_gaps", "a2a_sweep", "a2a_downlink", "a2a_reaction",
                    "a2a_ackroute", "a2a_noack", "a2a_hostlink", "a2a_downlink_spray", "a2a_twoend"):
            stages = {"a2a_compute_gaps": "a2a_probe_4k", "a2a_sweep": "a2a_sweep_4k",
                      "a2a_downlink": "a2a_downlink_4k", "a2a_reaction": "a2a_reaction_4k",
                      "a2a_ackroute": "a2a_ackroute_4k", "a2a_noack": "a2a_noack_4k",
                      "a2a_hostlink": "a2a_hostlink_4k", "a2a_downlink_spray": "a2a_downlink_spray_4k",
                      "a2a_twoend": "a2a_twoend_4k"}
            x.add_argument("--stage", default=stages.get(name, name))
            x.add_argument("--markdown", type=Path)
        if name == "a2a_ackroute":
            x.add_argument("--ack-route", choices=A2A_ACK_ROUTES)
        if name in ("a2a_ackroute", "a2a_twoend"):
            x.add_argument("--with-root", type=Path)
            x.add_argument("--with-stage",
                           default=A2A_ACKROUTE_WITH_STAGE if name == "a2a_ackroute" else A2A_TWOEND_WITH_STAGE)
        if name == "a2a_hostlink":
            x.add_argument("--with-root", type=Path, nargs="*", default=[])
            x.add_argument("--with-stage", default=A2A_HOSTLINK_WITH_STAGE)
        if name == "a2a_noack":
            x.add_argument("--ack-root", type=Path, nargs="*", default=[])
        if name == "a2a_sweep":
            x.add_argument("--shared-root", type=Path)
        if name in ("a2a_candidates", "a2a_cycles"):
            x.add_argument("--stage")
            x.add_argument("--markdown", type=Path)
        if name == "a2a_calibrate":
            x.add_argument("--into", type=Path, default=Path(__file__).resolve().parent / "a2a_calibration.json")
    arguments = parser.parse_args()
    if arguments.command == "a2a_calibrate":
        existing = json.loads(arguments.into.read_text()) if arguments.into.exists() else {}
        result = a2a_calibrate(arguments.runs, arguments.out_root, existing)
        arguments.into.write_text(json.dumps(result, indent=1) + "\n")
        print(json.dumps(result, indent=1))
        return 0
    if arguments.command == "cost":
        rows = [r for path in arguments.runs for r in transient_rows(path, arguments.out_root, arguments.stage)]
        a2a = a2a_rows(arguments.runs, arguments.out_root, arguments.stage)
        print(json.dumps(cost_cells(rows, arguments.reference) + a2a_cost_cells(a2a, arguments.reference), indent=1))
        return 0
    if arguments.command == "a2a_gate":
        rows = [r for r in a2a_rows(arguments.runs, arguments.out_root, arguments.stage)
                if r["arm"] in ("uniform", A2A_ORACLE)]
        verdicts = a2a_gate(a2a_cost_cells(rows))
        if arguments.json:
            print(json.dumps(verdicts, indent=1))
        else:
            for row in verdicts:
                print(gate_line(row))
        return 0
    if arguments.command == "a2a_gate_reps":
        rows = [r for r in a2a_rows(arguments.runs, arguments.out_root, arguments.stage)
                if r["arm"] in (A2A_REPS_ORACLE, "reps")]
        verdicts = a2a_gate_reps(a2a_cost_cells(rows))
        if arguments.markdown:
            arguments.markdown.write_text(gate_reps_markdown(verdicts))
        print(json.dumps(verdicts, indent=1))
        return 0
    if arguments.command == "a2a_candidates":
        cells = a2a_candidates(a2a_rows(arguments.runs, arguments.out_root, arguments.stage), arguments.out_root)
        if arguments.markdown:
            arguments.markdown.write_text(candidates_markdown(cells))
        print(json.dumps(cells, indent=1))
        return 0
    if arguments.command == "a2a_reaction":
        cells = a2a_reaction(a2a_rows(arguments.runs, arguments.out_root, arguments.stage), arguments.out_root)
        if arguments.markdown:
            arguments.markdown.write_text(reaction_markdown(cells))
        print(json.dumps(cells, indent=1))
        return 0
    if arguments.command == "a2a_downlink":
        cells = a2a_downlink(a2a_rows(arguments.runs, arguments.out_root, arguments.stage), arguments.out_root)
        if arguments.markdown:
            arguments.markdown.write_text(downlink_markdown(cells))
        print(json.dumps(cells, indent=1))
        return 0
    if arguments.command == "a2a_ackroute":
        rows = a2a_rows(arguments.runs, arguments.out_root, arguments.stage)
        if arguments.with_root:
            rows += a2a_root_rows(arguments.with_root, arguments.with_stage)
        cells = a2a_ackroute(rows, arguments.out_root, arguments.ack_route)
        if arguments.markdown:
            arguments.markdown.write_text(ackroute_markdown(cells))
        print(json.dumps(cells, indent=1))
        return 0
    if arguments.command == "a2a_noack":
        cells = a2a_noack(a2a_rows(arguments.runs, arguments.out_root, arguments.stage), arguments.out_root,
                          a2a_ack_rows(arguments.ack_root))
        if arguments.markdown:
            arguments.markdown.write_text(noack_markdown(cells))
        print(json.dumps(cells, indent=1))
        return 0
    if arguments.command == "a2a_downlink_spray":
        cells = a2a_downlink_spray(a2a_rows(arguments.runs, arguments.out_root, arguments.stage),
                                   arguments.out_root)
        if arguments.markdown:
            arguments.markdown.write_text(downlink_spray_markdown(cells))
        print(json.dumps(cells, indent=1))
        return 0
    if arguments.command == "a2a_twoend":
        rows = a2a_rows(arguments.runs, arguments.out_root, arguments.stage)
        if arguments.with_root:
            rows += a2a_root_rows(arguments.with_root, arguments.with_stage)
        cells = a2a_twoend(rows, arguments.out_root)
        if arguments.markdown:
            arguments.markdown.write_text(twoend_markdown(cells))
        print(json.dumps(cells, indent=1))
        return 0
    if arguments.command == "a2a_hostlink":
        rows = a2a_rows(arguments.runs, arguments.out_root, arguments.stage)
        for root in arguments.with_root:
            rows += a2a_root_rows(root, arguments.with_stage)
        cells = a2a_hostlink(rows, arguments.out_root)
        if arguments.markdown:
            arguments.markdown.write_text(hostlink_markdown(cells))
        print(json.dumps(cells, indent=1))
        return 0
    if arguments.command == "a2a_sweep":
        cells = a2a_sweep(a2a_rows(arguments.runs, arguments.out_root, arguments.stage), arguments.out_root,
                          arguments.shared_root)
        if arguments.markdown:
            arguments.markdown.write_text(sweep_markdown(cells))
        print(json.dumps(cells, indent=1))
        return 0
    if arguments.command == "a2a_compute_gaps":
        rows = a2a_compute_gaps(arguments.runs, arguments.out_root, arguments.stage)
        if arguments.markdown:
            arguments.markdown.write_text(compute_gaps_markdown(rows))
        print(json.dumps(rows, indent=1))
        return 0
    if arguments.command == "a2a_cycles":
        cells = a2a_cycles(a2a_rows(arguments.runs, arguments.out_root, arguments.stage), arguments.out_root)
        if arguments.markdown:
            arguments.markdown.write_text(cycles_markdown(cells))
        print(json.dumps(cells, indent=1))
        return 0
    if arguments.command in ("calibrate", "gains"):
        existing = json.loads(arguments.into.read_text()) if arguments.into and arguments.into.exists() else {}
        if arguments.command == "calibrate":
            result = calibrate(arguments.runs, arguments.out_root, existing, arguments.stage or "calibration")
        else:
            result = gains(arguments.runs, arguments.out_root, existing)
        if arguments.into:
            arguments.into.write_text(json.dumps(result, indent=1) + "\n")
        print(json.dumps(result, indent=1))
        return 0
    if arguments.command == "versus":
        print(json.dumps(versus(arguments.runs, arguments.out_root, arguments.arm_a, arguments.arm_b), indent=1))
        return 0
    if arguments.command == "identity":
        for row in identity(arguments.out_root, arguments.other_root, arguments.runs):
            print(f"{row['key']}: {row['status']}" + "".join(f"; {d}" for d in row["differences"]))
        return 0
    if arguments.command == "select":
        if arguments.stage and arguments.stage.startswith("t_"):
            result = select_by_cost(transient_rows(arguments.runs, arguments.out_root, arguments.stage))
        else:
            result = select(arguments.runs, arguments.out_root)
        print(json.dumps(result, indent=1))
        return 0
    if arguments.command == "thresholds":
        metrics = ("mark_fraction", "loss_fraction") if arguments.switch_queue else ("mark_fraction", "trim_fraction")
        result = thresholds(arguments.runs, metrics)
    elif arguments.command == "gain":
        result = {run.name: all_top_fraction(run) for run in arguments.runs}
    elif arguments.command == "binding":
        result = {
            run.name: binding(run, 200e9 if "half_rate" in run.name else 400e9) for run in arguments.runs
        }
    else:
        result = compare(arguments.runs, arguments.out_root, arguments.stage)
    print(json.dumps(result, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
