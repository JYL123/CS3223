# CS3223
### CS3223 Project

This project is a simple `SPJ` (Select-Project-Join) query engine, which consists of the following features:
* Dynamic programming optimizer 
* Random optimizer
* Distinct
* Group by
* Hash join
* Block nested loop join
* Page nested loop join

There are some implementation consideration to be noted:
* The `Hash Join` algorithm will recursively hash the elements in the same partition, and if the number of recursive partition is more than 3 times and still need more hashing, to speed up the program,
the program will think the plan with hash join is infeasible, and the plan will choose `Block Nested Loop` instead.

  * This is due to, first, the limitation of the hash function, given a huge number of values the possibility of collision is high, second the number of duplicated values.

 To get started with this project as a `user`, please follow [this link](https://www.comp.nus.edu.sg/~tankl/cs3223/project/user.htm)

  To get started with this project as a `developer`, please follow [this link](https://www.comp.nus.edu.sg/~tankl/cs3223/project/developer.htm) 