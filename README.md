# Introduction

`mdq` is a Python library and programs for running molecular dynamics
in a distributed environment in a robust manner, featuring task replication and multiple MD backend.
Currently only GROMACS is supported, but future support for NAMD and others is planned.

# Dependencies

* [CCTools](http://www3.nd.edu/~ccl/software/download.shtml)
  `mdq` uses features in CCTools that haven't been released yet.
  Install either from the [master development branch][cctools-git] (untested)
  or from [my fork][cctools-badi]
* [mdprep](https://github.com/badi/mdprep)
* [pwq](https://github.com/badi/pwq)
* [guamps](https://github.com/badi/guamps)
* [GROMACS](http://www.gromacs.org/)

[cctools-git]: https://github.com/cooperative-computing-lab/cctools/tree/master
[cctools-badi]: https://github.com/badi/cctools/tree/3.7.X-badi

# Program Usage

The main `mdq` program provides subcommands for configuring, adding files, and running MD simulations.
Long simulations are be broken into short "generations" can may each run for hours or days.
This way a long running simulation (perhaps weeks or months) can run in an unreliable environment.

The general workflow is to run `mdq init`, followed by several calls to `mdq add`, before running `mdq prepare` to convert to `mdq` format, then `mdq run` will submit the jobs.
Finally, `mdq cat` can concatenate the resulting trajectory files.

## Example

### `init`

To start, we are going to run several gromacs simulations.
The input .tpr files must have already been generated.
Additionally, we can continue from a previously run simulation by providing
the path to the .trr trajectory file

```bash
$ mdq init \
   gromacs \ # this will be a gromacs simulation
   -g 10   \ # run for 10 generations
   -t 2000 \ # each generation runs for 2 nanoseconds
   -o 500  \ # write to the trajectory file every 500 picoseconds
   -c 12   \ # each simulation should use 12 cores
```

### `add`

We can now add different parameters to simulate.
For example, we may want to run a folding and unfolding simulation.

```bash
$ mdq add -T folded.tpr
$ mdq add -T unfolded.tpr
```

### `prepare`

Up until this point, `mdq` has only recorded specifications of the simulations to be run.
The `prepare` command will convert the specifications into the necesarry input files to run each simulation task.

```bash
$ mdq prepare
```


### `run`

We can now run the simulations.
The `run` command will generate, submit, and manage WorkQueue tasks.
By using WorkQueue, the resource pool may be widened (PBS, SGE, Condor, etc),
includes reliability (tasks stopped due to machine failure are automatically rescheduled),
and long simulations automatically managed as a series of short generations.
Additionally, if the `mdq` program is killed for some reason, rerunning `mdq run` will resume.

```bash
$ mdq run \
   -p 9123 \ # run on port 9123
   -l wq.log # write WQ statistics to this file (num workers, tasks, etc)
```

At this point the simulations have only been submitted to a queue.
In order for them to execute we need to start workers.

```bash
$ work_queue_worker \
   -d all \ # print all debugging information
   localhost \ # the hostname of the machine where 'mdq run` was executed
   9123 # the port that mdq is running on
```

### `cat`

As the generations of the simulations complete we may want to analyze them as a single trajectory.
The `mdq cat` program concatenates the components of the simulations into a single file.

```bash
$ mdq cat \
  --xtc   \ # concatenate the .xtc files
  -o sims   # put the files in this directory
```











