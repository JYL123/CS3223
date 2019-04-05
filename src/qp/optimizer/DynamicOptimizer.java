/** performs dynamic programming optimization **/

package qp.optimizer;

import qp.utils.*;
import qp.operators.*;
import java.lang.Math;
import java.util.Vector;

public class DynamicOptimizer{

    SQLQuery sqlquery;
    int numBuffer;

    public DynamicOptimizer(SQLQuery sqlquery, int numBuffer){
        this.sqlquery = sqlquery;
        this.numBuffer = numBuffer;
    }

    public Operator getOptimizedPlan(){

        DynamicPlan dp = new DynamicPlan(sqlquery, numBuffer);
        return dp.preparePlan();
    }

}