- Modify the "CODEROOT" variable in makefile.inc to point to the root of your codebase. Usually, this is not necessary.

- Copy your own implementation of rbf, ix, and rm to folder, "rbf", "ix", and "rm", respectively.
  Don't forget to include RM extension parts in the rm.h file after you copy your code into "rm" folder.
  
- Implement the extension of Relation Manager (RM) to coordinate data files and the associated indices of the data files.

- Also, implement Query Engine (QE)

   Go to folder "qe" and type in:

    make clean
    make
    ./qetest_01

   The program should work. But it does nothing until you implement the extension of RM and QE.


- By default you should not change those classes defined in rm/rm.h and qe/qe.h. If you think some changes are really necessary, please contact us first.
