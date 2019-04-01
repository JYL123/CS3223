/** prepares a random initial plan for the given SQL query **/
/** see the ReadMe file to understand this **/

package qp.optimizer;

import qp.utils.*;
import qp.operators.*;
import java.util.Vector;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.Enumeration;
import java.io.*;

public class DynamicInitialPlan{

    SQLQuery sqlquery;

    Vector projectlist;
    Vector fromlist;
    Vector selectionlist;     //List of select conditons
    Vector joinlist;          //List of join conditions
    Vector groupbylist;
    int numJoin;    // Number of joins in this query


    Hashtable tab_op_hash;          //table name to the Operator
    Operator root; // root of the query plan tree

    HashMap<String, HashMap<String, ArrayList<Condition>>> joins; // all lefttable-joincondition-righttable relationships
    ArrayList<String> joinTablesList; // all distinct tables for a join condition
    HashMap<ArrayList<String>, double> costTable; //to record each combination and its min cost


    public DynamicInitialPlan(SQLQuery sqlquery){
        this.sqlquery=sqlquery;

        projectlist=(Vector) sqlquery.getProjectList();
        fromlist=(Vector) sqlquery.getFromList();
        selectionlist= sqlquery.getSelectionList();
        joinlist = sqlquery.getJoinList();
        groupbylist = sqlquery.getGroupByList();
        numJoin = joinlist.size();

        joinTablesList = new ArrayList<>();
    }

    /** number of join conditions **/
    public int getNumJoins(){
        return numJoin;
    }


    /** prepare initial plan for the query **/
    public Operator prepareInitialPlan(){

        tab_op_hash = new Hashtable();

        createScanOp();
        createSelectOp();
        if(numJoin !=0){
            createJoinOp();
        }
        createProjectOp();
        return root;
    }


    /** Create Scan Operator for each of the table
     ** mentioned in from list
     **/
    public void createScanOp(){
        int numtab = fromlist.size();
        Scan tempop = null;

        for(int i=0;i<numtab;i++){  // For each table in from list


            String tabname = (String) fromlist.elementAt(i);
            Scan op1 = new Scan(tabname,OpType.SCAN);
            tempop = op1;


            /** Read the schema of the table from tablename.md file
             ** md stands for metadata
             **/
            String filename = tabname+".md";
            try {
                ObjectInputStream _if = new ObjectInputStream(new FileInputStream(filename));
                Schema schm = (Schema) _if.readObject();
                op1.setSchema(schm);
                _if.close();
            } catch (Exception e) {
                System.err.println("RandomInitialPlan:Error reading Schema of the table" + filename);
                System.exit(1);
            }
            tab_op_hash.put(tabname,op1);
        }

        // 12 July 2003 (whtok)
        // To handle the case where there is no where clause
        // selectionlist is empty, hence we set the root to be
        // the scan operator. the projectOp would be put on top of
        // this later in CreateProjectOp
        if ( selectionlist.size() == 0 ) {
            root = tempop;
            return;
        }

    }


    /** Create Selection Operators for each of the
     ** selection condition mentioned in Condition list
     **/
    public void createSelectOp(){
        Select op1 = null;

        for(int j=0;j<selectionlist.size();j++){

            Condition cn = (Condition) selectionlist.elementAt(j);
            if(cn.getOpType() == Condition.SELECT){
                String tabname = cn.getLhs().getTabName();
                //System.out.println("RandomInitial:-------------Select-------:"+tabname);

                Operator tempop = (Operator)tab_op_hash.get(tabname);
                op1 = new Select(tempop,cn,OpType.SELECT);
                /** set the schema same as base relation **/
                op1.setSchema(tempop.getSchema());

                modifyHashtable(tempop,op1);
                //tab_op_hash.put(tabname,op1);

            }
        }
        /** The last selection is the root of the plan tre
         ** constructed thus far
         **/
        if(selectionlist.size() != 0)
            root = op1;
    }

    /** create join operators **/
    public void createJoinOp(){
        joins = new HashMap<String, HashMap<String, ArrayList<Condition>>>();

        /** enumeration of single relation plans*/
        for (int i = 0; i < numJoin; i ++) {
            /** two tables join consists of lefttable, condition, righttable*/
            Condition cn = (Condition) joinlist.elementAt(i);
            String lefttab = cn.getLhs().getTabName();
            String righttab = ((Attribute) cn.getRhs()).getTabName();

            /** store all joins **/
            HashMap<String, ArrayList<Condition>> rightTableWithCon = new HashMap<>();
            /** there may be multiple conditions between left and right tables **/
            ArrayList<Condition> conditionList = new ArrayList<>();

            if(joins.containsKey(lefttab)) {

                rightTableWithCon = joins.get(lefttab);

                if(rightTableWithCon.containsKey(righttab)) {
                    /** add new condition to the old condition list*/
                    conditionList = rightTableWithCon.get(righttab);
                    if(!conditionList.contains(cn)) {
                        conditionList.add(cn);

                        /** update the hashmap*/
                        HashMap<String, ArrayList<Condition>> newRightTableWithCon = new HashMap<String, ArrayList<Condition>>();
                        newRightTableWithCon.put(righttab, oldCon);
                        joins.replace(lefttab, newRightTableWithCon);
                    }
                } else {
                    conditionList.add(cn);
                    rightTableWithCon.put(righttab, conditionList);
                    joins.put(lefttab, rightTableWithCon);
                }
            }

            conditionList.add(cn);
            rightTableWithCon.put(righttab, cn);
            joins.put(lefttab, rightTableWithCon);
        }

        /** to form all set of plans of size i, evaluate their cost, retain the cheapest plan for each combination**/
        for (int i = 2; i <= joinTablesList.size(); i ++) {
            ArrayList<ArrayList<String>> combinations = getCombinations(joinTablesList, i);


            /** compute cost for each permutation of tables of size i, get the min cost physical plan for the combination*/
            for (ArrayList<String> combination : combinations) {

                ArrayList<String> minLhsJoin = new ArrayList<>();
                ArrayList<String> minRhsJoin = new ArrayList<>();
                double minCost = Double.MAX_VALUE;

                ArrayList<ArrayList<ArrayList<String>>> plans= generatePlans(combination);
                ArrayList<String> lhsJoin = plans.get(j).get(0);
                ArrayList<String> rhsJoin = plans.get(j).get(1);

                int joinType = 0; //nestedLoop

                for (int j=0; j<plans.size(); j++) {

                    double cost = joinPlanCost(lhsJoin, rhsJoin, joinType);

                    if (cost < minCost) {
                        minCost = cost;
                        // record down the lhs and rhs as well
                        minLhsJoin = lhsJoin;
                        minRhsJoin = rhsJoin;
                    }
                }

                /** record down the min cost for this combination*/
                costTable.put(combination, minCost);

                /** join table, use tab_op_hash as joinPan set*/
                Operator left = (Operator) tab_op_hash.get(lhsJoin);
                Operator right = (Operator) tab_op_hash.get(rhsJoin);
                Join jn = new Join(left,right,cn,OpType.JOIN);
                Schema newsche = left.getSchema().joinWith(right.getSchema());
                jn.setSchema(newsche);
                jn.setJoinType(joinType);
                modifyHashtable(left,jn);
                modifyHashtable(right,jn);
            }

        }

        /** The last join operation is the root for the
         ** constructed till now
         **/

        if(numJoin !=0)
            root = jn;

    }


    public void createProjectOp(){
        Operator base = root;
        if ( projectlist == null )
            projectlist = new Vector();

        if(!projectlist.isEmpty()){
            root = new Project(base,projectlist,OpType.PROJECT);
            Schema newSchema = base.getSchema().subSchema(projectlist);
            root.setSchema(newSchema);
        }
    }

    private void modifyHashtable(Operator old, Operator newop){
        Enumeration e=tab_op_hash.keys();
        while(e.hasMoreElements()){
            String key = (String)e.nextElement();
            Operator temp = (Operator)tab_op_hash.get(key);
            if(temp==old){
                tab_op_hash.put(key,newop);
            }
        }
    }

    /** recursively generate all combinations of different number of tables*/
    private ArrayList<ArrayList<String>> getCombinations(ArrayList<String> tableList, int size){
        ArrayList<ArrayList<String>> results = new ArrayList<ArrayList<String>>();

        ArrayList<String> result = new ArrayList<String>();
        recursiveCombination(tableList, size, result, results);

        return results;

    }

    private void recursiveCombination(ArrayList<String> tableList, int size, ArrayList<String> result, ArrayList<ArrayList<String>> results){
        if(result.size() == size) {
            results.add(result);
            return;
        }

        for (int i = 0; i < tableList.size(); i++)
        {
            result.add(tableList.get(i));
            recursiveCombination(tableList, size, result, results);
        }
    }

    /** partition combination to lhsplans and rhsplans, which form a plan*/
    private ArrayList<ArrayList<ArrayList<String>>> generatePlans(ArrayList<String> combination) {

        ArrayList<ArrayList<ArrayList<String>>> plans = new ArrayList<>();

        for (int i=1; i<combination.size(); i++) {
            ArrayList<ArrayList<String>> lhsPlans = generateCombination(sources, i);

            // partition combination to lhsplans and rhsplans
            for (int j=0; j<lhsPlans.size(); j++) {
                ArrayList<String> lhs = lhsPlans.get(j);
                ArrayList<String> rhs = new ArrayList<String>();

                for (int t=0; t<combination.size(); t++) {
                    if (!lhs.contains(combination.get(t))) {
                        rhs.add(combination.get(t));
                    }
                }
                ArrayList<ArrayList<String>> plan = new ArrayList<>();
                plan.add(lhs);
                plan.add(rhs);
                plans.add(plan);
            }
        }
        return plans;
    }

    private long joinPlanCost(ArrayList<String> lhsPlan, ArrayList<String> rhsPlan, int joinType) {
        // sub-plan must always inside the memo table
        double lhsPlanCost = 0;
        double rhsPlanCost = 0;

        lhsPlanCost = costTable.get(lhsPlan);
        rhsPlanCost = costTable.get(rhsPlan);

        /** TODO: get page numbers of joint tables from the combined plans*/
        int lhspages = 0;
        int rhspages = 0;


        double joinCost = 0;
        /** TODO: modify cost computation regarding to each join method*/
        switch(joinType){
            case JoinType.NESTEDJOIN:
                joincost = lhspages*rhspages;
                break;
            case JoinType.BLOCKNESTED:
                joincost = 0;
                break;
            case JoinType.SORTMERGE:
                joincost = 0;
                break;
            case JoinType.HASHJOIN:
                joincost = 0;
                break;
            default:
                joincost=0;
                break;
        }

        return lhsPlanCost + rhsPlanCost + joinCost;
    }


}



