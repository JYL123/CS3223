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

        DynamicInitialPlan dip = new DynamicInitialPlan(sqlquery);
        return dip.prepareInitialPlan();
    }

}