/** performs dynamic programming optimization **/

package qp.optimizer;

import qp.utils.*;
import qp.operators.*;
import java.lang.Math;
import java.util.Vector;

public class DynamicOptimizer{

    SQLQuery sqlquery;

    public DynamicOptimizer(SQLQuery sqlquery){
        this.sqlquery = sqlquery;
    }

    public Operator getOptimizedPlan(){

        DynamicPlan dp = new DynamicPlan(sqlquery);
        return dp.preparePlan();
    }

    /** AFter finding a choice of method for each operator
     prepare an execution plan by replacing the methods with
     corresponding join operator implementation
     **/
    public static Operator makeExecPlan(Operator node){

        if(node.getOpType()==OpType.JOIN){
            Operator left = makeExecPlan(((Join)node).getLeft());
            Operator right = makeExecPlan(((Join)node).getRight());
            int joinType = ((Join)node).getJoinType();
            int numbuff = BufferManager.getBuffersPerJoin();
            switch(joinType){
                case JoinType.NESTEDJOIN:

                    NestedJoin nj = new NestedJoin((Join) node);
                    nj.setLeft(left);
                    nj.setRight(right);
                    nj.setNumBuff(numbuff);
                    return nj;

                /** Temporarity used simple nested join,
                 replace with hasjoin, if implemented **/

                case JoinType.BLOCKNESTED:

                    NestedJoin bj = new NestedJoin((Join) node);
                    /* + other code */
                    return bj;

                case JoinType.SORTMERGE:

                    NestedJoin sm = new NestedJoin((Join) node);
                    /* + other code */
                    return sm;

                case JoinType.HASHJOIN:

                    NestedJoin hj = new NestedJoin((Join) node);
                    /* + other code */
                    return hj;
                default:
                    return node;
            }
        }else if(node.getOpType() == OpType.SELECT){
            Operator base = makeExecPlan(((Select)node).getBase());
            ((Select)node).setBase(base);
            return node;
        }else if(node.getOpType() == OpType.PROJECT){
            Operator base = makeExecPlan(((Project)node).getBase());
            ((Project)node).setBase(base);
            return node;
        }else{
            return node;
        }
    }

}