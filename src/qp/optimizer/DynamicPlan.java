/** prepares a random initial plan for the given SQL query **/
/** see the ReadMe file to understand this **/

package qp.optimizer;

import java.io.*;
import java.lang.Math;
import qp.operators.*;
import qp.utils.*;
import java.util.Vector;
import java.util.BitSet;
import java.util.Hashtable;
import java.util.Enumeration;

public class DynamicPlan{

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
    ArrayList<Table> jointTablesList; // stores all tables
    ArrayList<Attribute> jointAttributesList; // all distinct table's attribute for a join condition
    HashMap<ArrayList<String>, double> costTable; //to record each combination and its min cost
    HashMap<ArrayList<String>, int> intermediateTablePages; // to record down the number of pages in each intermeidate table


    public DynamicPlan(SQLQuery sqlquery){
        this.sqlquery=sqlquery;

        projectlist=(Vector) sqlquery.getProjectList();
        fromlist=(Vector) sqlquery.getFromList();
        selectionlist= sqlquery.getSelectionList();
        joinlist = sqlquery.getJoinList();
        groupbylist = sqlquery.getGroupByList();
        numJoin = joinlist.size();


        /** created for JOIN*/
        jointAttributesList = new ArrayList<Attribute>();
        populateJointAttributesList(jointAttributesList);
        costTable = new HashMap<ArrayList<String>, double>();
        jointTablesList = new ArrayList<Table>();
        populateJointTableList(jointTablesList);
    }

    /** number of join conditions **/
    public int getNumJoins(){
        return numJoin;
    }


    /** prepare initial plan for the query **/
    public Operator preparePlan(){

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
                System.err.println("DynamicPlan:Error reading Schema of the table" + filename);
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

        /** initialize cost table with all tales from FROM list, and its cost as number of batches in the table**/
        for (int i = 1; i <= joinlist.size(); i ++) {
            //costTable = new HashMap<ArrayList<String>, double>();
            Condition cn = (Condition) joinlist.elementAt(jnnum);

            /** get attribute for left and right combo*/
            Attribute leftAttr = (Attribute)cn.getLhs();
            Attribute rightAttr = (Attribute)cn.getLhs();


            ArrayList<String> lefttab = new ArrayList<String>();
            lefttab.add(leftAttr.getTabName());

            ArrayList<String> righttab = new ArrayList<String>();
            lefttab.add(rightAttr.getTabName());

            /** get number of pages*/
            double leftTuples= leftAttr.getNumCols();
            int leftBatchSize = (int) Math.floor(Batch.getPageSize()/leftAttr.setTupleSize());
            double leftCost = leftTuples/leftBatchSize;

            double rightTuples= rightAttr.getNumCols();
            int rightBatchSize = (int) Math.floor(Batch.getPageSize()/rightAttr.setTupleSize());
            double rightCost = rightTuples/rightBatchSize;

            if(!costTable.containsKey(lefttab)) costTable.put(lefttab, leftCost);
            if(!costTable.containsKey(righttab)) costTable.put(righttab, rightCost);
        }




        /** to form all set of plans of size i, evaluate their cost, retain the cheapest plan for each combination**/
        for (int i = 2; i <= jointAttributesList.size(); i ++) {

            ArrayList<String> tableNameList = new ArrayList<String>();
            populateTableNameList(tableNameList);

            /** each combnation consists of an array of table names*/
            ArrayList<ArrayList<String>> combinations = getCombinations(tableNameList, i);


            /** compute cost for each permutation of tables of size i, get the min cost physical plan for the combination*/
            for (ArrayList<String> combination : combinations) {

                ArrayList<String> minLhsJoin = new ArrayList<>();
                ArrayList<String> minRhsJoin = new ArrayList<>();
                double minCost = Double.MAX_VALUE;

                ArrayList<ArrayList<ArrayList<String>>> plans= generatePlans(combination);

                /** TODO: hardcoded jointype, 0 stands for nestedLoop*/
                int joinType = 0;

                for (int j=0; j<plans.size(); j++) {
                    ArrayList<String> lhsJoin = plans.get(j).get(0);
                    ArrayList<String> rhsJoin = plans.get(j).get(1);

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
                /** record down the resultant table(tablename, number of tuples, schema) for this combination*/
                jointTablesList.add(joinTables(minLhsJoin, minRhsJoin));
            }

        }

        /** The last join operation is the root for the
         ** constructed till now
         **/

        if(numJoin !=0)
            root = jn;

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
        // sub-plan must always inside the cost table
        double lhsPlanCost = costTable.get(lhsPlan);
        double rhsPlanCost = costTable.get(rhsPlan);

        String lhsPlanName = getCombinedAttributeName(lhsPlan);
        String rhsPlanName = getCombinedAttributeName(rhsPlan);

        int lhspages= 0;
        int rhspages = 0;

        for (Table table : jointTablesList) {
            if (table.getTableName().equals(lhsPlanName)) lhspages = (int) Math.ceil(table.getNumTuples()/Batch.getPageSize());
            if (table.getTableName().equals(rhsPlanName)) rhspages = (int) Math.ceil(table.getNumTuples()/Batch.getPageSize());
        }

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

    /** return a set of table's attriute, which will contain information used in cost computation*/
    private ArrayList<Attribute> populateJointAttributesList(ArrayList<Attribute> joinTablesList){

        for (int i = 1; i <= joinlist.size(); i ++) {
            //costTable = new HashMap<ArrayList<String>, double>();
            Condition cn = (Condition) joinlist.elementAt(jnnum);

            /** get attribute for left and right combo*/
            Attribute leftAttr = (Attribute) cn.getLhs();
            Attribute rightAttr = (Attribute) cn.getLhs();

            if(!joinTablesList.contains(leftAttr)) joinTablesList.add(leftAttr);
            if(!joinTablesList.contains(rightAttr)) joinTablesList.add(rightAttr);
        }

        return joinTablesList;
    }


    private ArrayList<String> populateTableNameList(ArrayList<String> tableNameList) {

        for (int i = 0; i < tableJoinlist.size(); i++) {

            Attribute attr = (Attribute) tableJoinlist.get(i);
            String tableName = attr.getTabName();

            if (!tableNameList.contains(tableName)) tableNameList.add(tableName);
        }

        return tableNameList;
    }

    private ArrayList<Table> populateJointTableList(ArrayList<Table> jointTablesList) {

        for(int i=0;i<fromlist.size();i++){  // For each table in from list


            String tabname = (String) fromlist.elementAt(i);
            //Scan op1 = new Scan(tabname,OpType.SCAN);
            //tempop = op1;


            /** Read the schema of the table from tablename.md file
             ** md stands for metadata
             **/

            String filename = tabname+".md";
            try {
                ObjectInputStream _if = new ObjectInputStream(new FileInputStream(filename));
                Schema schm = (Schema) _if.readObject();
                Table table = new Table(schm.getNumCols(), tabname, schm);
                jointTablesList.add(table);
                _if.close();
            } catch (Exception e) {
                System.err.println("populateJointTableList:Error reading Schema of the table" + filename);
                System.exit(1);
            }
        }

        return jointTablesList;
    }

    private String getCombinedAttributeName(ArrayList<String> combination) {

        String combinedName = "";
        for (String tab : combination) {
            combinedName = combinedName + tab;
        }

        return combinedName;
    }

    private Table joinTables(ArrayList<String> minLhsJoin, ArrayList<String> minRhsJoin) {

        String lhsName = getCombinedAttributeName(minLhsJoin);
        String rhsName = getCombinedAttributeName(minRhsJoin);

        // new table name
        String newTableName = lhsName + rhsName;

        Table leftTab = null;
        Table rightTab = null;

        for (Table table : jointTablesList) {
            /** will it possible that the table is not in the list? */
            if (table.getTableName().equals(lhsName)) leftTab = table;
            if (table.getTableName().equals(rhsName)) rightTab = table;
        }

        //new schema
        Schema newSchema = leftTab.getSchema().joinWith(rightTab.getSchema());

        //new tuple number
        int newTupleNum = newSchema.getNumCols();

        return new Table(newTupleNum, newTableName, newSchema);
    }


    // Table structure for cost computation
    class Table {
        int numTuples;
        String tableName;
        Schema schema;

        public Table(int numTuples, String tableName, Schema schema) {
            this.numTuples = numTuples;
            this.tableName = tableName;
            this.schema = schema;
        }

        public int getNumTuples() return numTuples;

        public void setNumTuples(int numTuples) {
            this.numTuples = numTuples;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public void setSchema(Schema schema) {
            this.schema = schema;
        }

        public Schema getSchema() {
            return schema;
        }
    }
}



