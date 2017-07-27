# Running this Evaluation

## BSBM named graph handling

To correctly write the data to the named graph, we have added the graph pattern to `queries/update/query1.txt` and `queries/update/query2.txt`:

    INSERT DATA {
    GRAPH <urn:bsbm> { %updateData% }
    }

    DELETE WHERE
    { GRAPH <urn:bsbm> { %Offer% ?p ?o } }

This can be done by changing to the bsbm directory and running

    patch -p1 < <quit-eval directory>/bsbm-queries.patch

TODO: we should check if this is really needed, or if we can correctly evaluate the URL parameter sent by the testdriver

## Setup data and monitor execution

We expect this repository to be directly checked out into a directory named `quit-eval`, which resides next to the `bsbmtools-0.2` directory.

Create BSBM datasets

    # Run inside the bsbmtools-0.2 directory
    ./generate -pc 1000 -ud -tc 1000 -ppt 1

Create a scenario description. You can use `scenario.yml.example` as a template.

Start the test runs:

    $ pip install -r requirements.txt
    $ ./bsqbm.py scenario.yml

# Verification of a quit repository after the bsbm execution

reset the repository to the initial commit (currently the initial commit is not tagged with the `./bsqbm.py` script so you have to find and tag it on your own.)

    git checkout init-graph

    sed "s/.$/<urn:bsbm> ./g" ../bsbmtools-0.2/dataset.nt | LC_ALL=C sort -u > graph.nq
    ./verify.py <quit_repo>-log/run….log <quit_repo> graph.nq

# Verification of the Merge strategy

## Configure Quit Merge Strategy:

Add these lines to your `~/.gitconfig`

    [merge "quitmerge"]
    name = Quit – Quads in Git – merge driver
    driver = quit-merge %O %A %B

and these lines to a `.gitattributes` file in a git repository:

    *.nt    merge=quitmerge
    *.nq    merge=quitmerge

1. create an empty git repository
2. add a `.gitattributes` file which points to the configured *QuitMerge* strategy.
3. get the `dataset.nq` file from above
4. run `./merge.py`

    ./merge.py <the git repo> <path to graph.nq> [--seed <any seed>] [--result <the correct merge result>]

(If you want to verify the merge result your self, you can use the `--result` option to dump the expected result)

# Formate results for gnuplot

The script `./evaluate.py` helps in this case.

For generating the output regarding QMpH and QpS run

    ./evaluate.py --bsbm

For aligning the memory log of the run with the number of commits at this point in time run:

    ./evaluate.py --align <name of the run> > <name of the run>.dat

Where `<name of the run>` is the directory name of the respective run without the `-log` suffix. E.g. `quit-gc-2`.

If you don't have GitPython installed you can use `pip install -r requirements.txt`.

# Results

This was a run on a laptop with:
* CPU: Intel Core i7-5600U, 2.6GHz, two physical cores (4 virtual cores)
* SSD: SAMSUNG MZ7LN512HCHP-000L1, 476.94GiB
* RAM 15.4GiB, 1600MHz
* OS: Debian GNU/Linux 8 (jessie) 64-Bit

I've ran the bsbm with 40 warmup runs and 1500 querymix runs

[![](figures/mem.png)](figures/mem.pdf)

# Reproduce

The quit store should be on commit 7aae256b2f8f41ae7ac4da363a8511aee43d9f24 to reproduce the result as printed above.

# License

Copyright (C) 2017 Natanael Arndt <http://aksw.org/NatanaelArndt> and Norman Radtke <http://aksw.org/NormanRadtke>

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program; if not, see <http://www.gnu.org/licenses>.
Please see [LICENSE](LICENSE) for further information.
