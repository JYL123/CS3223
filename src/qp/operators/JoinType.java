/** Enumeration of join algorithm types
	Change this class depending on actual algorithms
	you have implemented in your query processor 

**/


package qp.operators;

import qp.utils.*;
public class JoinType{



   public static final int NESTEDJOIN = 0;
   public static final int BLOCKNESTED = 2;
   public static final int SORTMERGE = 7;
   public static final int HASHJOIN = 1;
   public static final int INDEXNESTED = 8;

   public static int numJoinTypes(){
	return 2;
    //TODO
        // return k for k joins
    }

}

