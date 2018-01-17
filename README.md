# koji_runner

Custom Gitlab runner to run jobs on a [Koji](https://pagure.io/koji/) build system.  Live console output is captured just as in the regular runner - artifacts are captured as well.

Builds not based on a tag will be submitted as [scratch](https://docs.pagure.org/koji/using_the_koji_build_system/#scratch-builds) builds.
