# Overview

## Conceptualisation

### Background

Matchmaking in online video games is a classic example of a high-concurrency distributed computing problem. The design of such a system is crucial to the success of any game that has a competitive element: players are highly sensitive to the quality of games they receive, and if that quality is compromised by a poor matchmaking system, they will inevitably be turned away.

The longest standing postulate of matchmaking algorithms has been to match players as close as possible in terms of skill level, based on some skill rating system specific to the game. The underlying assumption is that players will find the highest quality of games when they are matched to players that are at/near their skill level. This is **skill-based matchmaking** (SBMM), and has been around for as long as competitive games requiring online matchmaking has been around.

### Aims

This project aims to simulate a matchmaking service that receives player enqueue requests and emits matched groupings of players that minimise some variance criterion on player skill.

## Exploration

### Scale

We turn to the most popular online titles of today to gather some base statistics, in particular the peak Concurrent Users (CCUs) that each game is expected to handle (most are estimates):

- Overwatch (all platforms, estimate): >1m CCU
- Counter-Strike 2: ~1.8m CCU
- DotA 2: ~1.3m CCU
- Apex Legends: ~620k CCU
- Marvel Rivals (excluding China): ~640k CCU

>[!info] Reliable Estimation
>
>Reliable estimates and historical CCU data for games are remarkably difficult to source and analyse, especially if they operate on platforms other than Steam. Steam provides full time-series historical CCU data, while almost every other large game company obscures said data and have a significant or entire portion of their player base play on their proprietary distribution platforms (data not captured on Steam).
>
>In the above, we use mostly use data from SteamDB and consider the largest games on there.

We place a first target at 1–2M CCUs.

### Format

Not all matchmaking systems are created equal. Different games have drastically different conditions for matchmaking.

Possibly the most popular modality is 5v5: 5 players per team, 2 teams going head to head. Almost all major titles that come to mind follow this format: LoL, DotA2, CS2, Overwatch, Rainbow 6 Siege etc.

Smaller formats do exist, but the limiting case is the most interesting: 1v1/head-to-head is a very common modality amongst more niche titles, especially fighting games. Even online chess uses 1v1 matchmaking. This fundamentally poses a much simpler problem, simply due to the reduction in interacting factors.

Another very popular modality is the battle royale: a large number of players (around 100) get matched into a single lobby, possibly grouped into teams or free-for-all. Such games tend to have significantly lower restrictions on skill rating variance within a match, not only for practical reasons (trying to find 99 other evenly-matched opponents for a very high-level player is nearly impossible in many games), but also because it is less important for such games to have very even matching.

Team-based games provide a larger level of complication. To make the problem tractable, it is better to split the problem into two stages: the match should be made first, then the players matched should be split into teams. Matching is a hard global problem that needs to be fast, while team splitting is not only very tractable with even highly general metrics, it is upper-bounded by a constant precisely because the match size is fixed at this point. Both problems are indeed worth solving; we will look at both separately.

### Factors

Matchmaking in most games are also complicated by additional factors introduced to support different game formats or systems that encourage sportsmanship. We can largely characterise these into two kinds of systems:

- Disjoint Segmentation (DS)
- Compositional Segmentation (CS)
- Multi-Queuing (MQ)

Disjoint Segmentation is the first such example: many games often segment their players into different queues, primarily for different games modes to form disjoint matches. Each game mode and queue may have their own unique matchmaking system, but more often than not they tend to be extremely similar (use the same underlying system) and vary only in algorithmic parameters like the target skill rating variance within a single match or the number of players to match per game.

Grouped Disjoint Segmentation (gDS) is an extension of DS that supports premade groups: players that queue together in the same party, and therefore must not only be kept within the same match, but also the same team. This requires a more low-level adaptation than offered by CS.

Compositional Segmentation is the opposite: players are segmented into distinct queues, but now to form the same match. This is used most commonly for systems like role queuing, which is particularly important in games where team composition significantly affects the game quality and balancing, and requires that a fixed number of players be used to fill each role.

Multi-Queueing is a more interesting one but also poses a much greater level of complication: this involves allowing players to join multiple queues at the same time, and search for a match any queue they are in. Matches can only be created from players within the same queue, and matching a player removes them (naturally) from all queues simultaneously. Games like Call of Duty and Battlefield, and even CS2 do this, where players are allowed to granularly choose which game modes, maps, etc. they want to play on and be enqueued for all simultaneously until they are placed into a game matching at least one combination of their selected choices.

We will start with DS, and explore CS and MQ later, in particular by reducing CS and MQ (and possibly more complex queuing structures) to DS.

### Metrics

To measure the performance of the matchmaker, we consider two main metrics.

The first is **turnaround time**, which is the time taken to match a player between submitting the enqueue request and finding them a match. Players value finding a game fast, so the time taken to match players needs to be low.

The second is **rating delta**, particularly within each match, which is the range of ratings encountered in a particular match. Following SBMM principles, this needs to be minimised across all matches.

These two metrics are often at odds with each other: forcing a low turnaround time often requires sacrificing low rating deltas, because trying to match players fast means matching between smaller pools of players. In contrast, allowing a higher turnaround time causes players to wait longer, but potentially allows more players to enter the queue and therefore a higher chance of matching with similarly-rated players, reducing rating deltas. Part of this project will explore how to balance between these two metrics.

**Rating Delta Metric**

To measure the 'badness' of a game with players of mismatched skill ratings, we consider that in general, the mismatches in a game are felt from interactions between players.

Therefore, we design the most general metric for rating mismatch badness to be

$ C(R) = sum_(S, T subset.eq R) phi(S, T), $

where $R$ is a sequence of ratings $r_1, r_2, ..., r_n$ and $phi$ produces the mismatch cost between any two subsets of players in a game. We require that $phi$ is commutative; that is, $phi(S, T) = phi(T, S)$ for any $S, T subset.eq R$.

In reality, the interactions that are most significant can be boiled down to two kinds:
1. Player-Player Interactions: in games like CS2, Rainbow 6 Siege, etc. characterised by one-on-one skirmishes, player-player interactions tend to dominate. In such cases, the effect is worst felt when there are mismatches between player skill ratings.
2. Team-Team Interactions: in games like Overwatch, DotA 2, etc. characterised by teamfights, team-team interactions tend to dominate. In such cases, the effect is worst felt when there are mismatches between aggregate team ratings.

In the first case, we only need to primarily consider pairwise interactions between players, combined with some smaller contribution from aggregations across teams. One easy statistical measure to use is the population variance in $R$, $Var(R)$.

In the second case, we only need to primarily consider differences across teams after aggregation, combined with some smaller contribution from pairwise interactions. As we have it, $Var(R)$ also happens to account for this well enough, but a more apparent problem looms: $R$ is not yet split into teams, so we can't consider this just yet; in fact, we shouldn't, as this is up to the match splitter algorithm (the one that forms the teams given a matched set of players).

As a replacement, we consider a secondary measure: the range of $R$, defined to be $max R - min R$. This gives us a good measure of maximum spread, and together with $Var(R)$, gives a very good summary of a matching's general quality by combining multiple spread measures.

We can fine-tune the contributions from simpler variability measures to get a composite score. This gives us our first usable metric:

$ hat(C)(R) = alpha Var(R) + beta (max R - min R)^gamma, $

where $alpha > 0, beta > 0, gamma >= 1$ are hyperparameters. This measure is naturally convex. For our cases we can choose $alpha = beta = 1$ and $gamma = 2$ as a good starting point (penalises the most extreme pair disproportionately, while accounting for variance).