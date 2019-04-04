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
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.io.File;
import java.util.Scanner;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import static java.util.Map.Entry;


public class DynamicPlan{

    SQLQuery sqlquery;

    Vector projectlist;
    Vector fromlist;
    Vector selectionlist;     //List of select conditons
    Vector joinlist;          //List of join conditions
    Vector groupbylist;
    int numJoin;    // Number of joins in this query
    int numBuffer;


    Hashtable tab_op_hash;          //table name to the Operator
    Operator root; // root of the query plan tree

    HashMap<String, HashMap<String, ArrayList<Condition>>> joins; // all lefttable-joincondition-righttable relationships
    ArrayList<Table> jointTablesList; // stores all tables
    ArrayList<Attribute> jointAttributesList; // all distinct table's attribute for a join condition
    ArrayList<String> tableNameList; // list of table names
    HashMap<ArrayList<String>, Double> costTable; //to record each combination and its min cost, cost = number of pages in a table
    HashMap<ArrayList<String>, Integer> intermediateTablePages; // to record down the number of pages in each intermeidate table
    ArrayList<String> currentResult;


    public DynamicPlan(SQLQuery sqlquery, int numBuffer){
        this.sqlquery=sqlquery;

        projectlist=(Vector) sqlquery.getProjectList();
        fromlist=(Vector) sqlquery.getFromList();
        selectionlist= sqlquery.getSelectionList();
        joinlist = sqlquery.getJoinList();
        groupbylist = sqlquery.getGroupByList();
        numJoin = joinlist.size();
        this.numBuffer = numBuffer;

        if(numJoin != 0){

            /** created for JOIN*/
            jointAttributesList = new ArrayList<Attribute>();
            populateJointAttributesList(jointAttributesList);
            jointTablesList = new ArrayList<Table>();
            populateJointTableList(jointTablesList);
            setTupleNumber(jointTablesList);
            for (Table table : jointTablesList) {
                System.out.println("This is one table: ");
                System.out.println(table.getTableName());
                System.out.println(table.getNumTuples());
                System.out.println(table.getSchema());
            }

            costTable = new HashMap<ArrayList<String>, Double>();
            populateCostTable(costTable);
            /** debug: print cost table*/
            for (ArrayList<String> tablelist : costTable.keySet()) {
                System.out.println("This is one cost entry, table: ");
                for (String table : tablelist) {
                    System.out.print(table + " ");
                }
            }
            for (Double cost : costTable.values()) {
                System.out.println("This is one cost entry, cost: ");

                System.out.println(cost + " ");

            }

            tableNameList = new ArrayList<String>();
            populateTableNameList(tableNameList);
            System.out.println("Table names in table name list: ");
            for (String tableName : tableNameList) {
                System.out.println(tableName);
            }
        }
    }

    /** number of join conditions **/
    public int getNumJoins(){
        return numJoin;
    }


    /** prepare initial plan for the query **/
    public Operator preparePlan(){

        tab_op_hash = new Hashtable();

        createScanOp();
        System.out.println("root after scan is: ");
        System.out.println(root);

        createSelectOp();
        System.out.println("root after select is: ");
        System.out.println(root);
        if(numJoin !=0){
            createJoinOp();
            System.out.println("root after join is: ");
            System.out.println(root);
        }

        createProjectOp();

        Debug.PPrint(root);
        PlanCost pc = new PlanCost();
        System.out.println("The cost for this plan is:  " + pc.getCost(root));

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

    /** create join operators **/
    public void createJoinOp(){

        System.out.println("DP Join...");

        Join jn=null;


        /** to form all set of plans of size i, evaluate their cost, retain the cheapest plan for each combination**/
        for (int i = 2; i <= fromlist.size(); i ++) {

            /** each combination consists of an array of table names*/
            ArrayList<ArrayList<String>> combinations = getCombinations(tableNameList, i);

            /** debug */
            System.out.println("combinations generated: ");
            for (ArrayList<String> combination : combinations) {
                System.out.println("This is one combination generated: ");
                for (String table : combination) {
                    System.out.print(table + " ");
                }
            }
            System.out.println("");

            /** compute cost for each combination of tables of size i, get the min cost physical plan for the combination*/
            for (ArrayList<String> combination : combinations) {

                System.out.println("We are looking at this plan: ");
                for(String combo : combination) {
                    System.out.print(combo + " ");
                }


                ArrayList<String> minLhsJoin = new ArrayList<>();
                ArrayList<String> minRhsJoin = new ArrayList<>();
                double minCost = Double.MAX_VALUE;

                ArrayList<ArrayList<ArrayList<String>>> plans= generatePlans(combination);

                /** debug the plans generated */
                System.out.println("the number of plans: ");
                System.out.println(plans.size());

                System.out.println("This is one plan: ");
                for (ArrayList<ArrayList<String>> plan : plans) {
                    System.out.println("This is left plan: ");
                    ArrayList<String> lhss = plan.get(0);
                    for (String lhssplan : lhss) {
                        System.out.print(lhssplan + " ");
                    }
                    System.out.println("");
                    System.out.println("This is right plan: ");
                    ArrayList<String> rhss = plan.get(1);
                    for (String rhssplan : rhss) {
                        System.out.print(rhssplan + " ");
                    }
                    System.out.println("");
                }

                for (int j=0; j<plans.size(); j++) {
                    ArrayList<String> lhsJoin = plans.get(j).get(0);
                    ArrayList<String> rhsJoin = plans.get(j).get(1);

                    double cost = joinPlanCost(lhsJoin, rhsJoin);

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

                /** set the node*/
                Operator left = (Operator) tab_op_hash.get(getCombinedTablesName(minLhsJoin));
                Operator right = (Operator) tab_op_hash.get(getCombinedTablesName(minRhsJoin));


                Condition cn = null;

                int conditionIndex = 0;
                for (int z = 0; z < joinlist.size(); z ++) {
                    Condition temp = (Condition) joinlist.elementAt(z);
                    if((temp.getLhs().getTabName()).equals(getCombinedTablesName(minLhsJoin))
                            || ((temp.getLhs().getTabName()).equals(getCombinedTablesName(minRhsJoin)))){
                        cn = temp;

                        if((temp.getLhs().getTabName()).equals(getCombinedTablesName(minLhsJoin))) {
                            jn = new Join(left,right,cn,OpType.JOIN);
                            Schema newsche = left.getSchema().joinWith(right.getSchema());
                            jn.setSchema(newsche);
                            int joinMeth = getJoinMethod(minLhsJoin, minRhsJoin);
                            jn.setJoinType(joinMeth);
                            String combinedName = getCombinedTablesName(minLhsJoin, minRhsJoin);
                            tab_op_hash.put(combinedName,jn);
                        }
                        if(((temp.getLhs().getTabName()).equals(getCombinedTablesName(minRhsJoin)))) {
                            jn = new Join(right,left,cn,OpType.JOIN);
                            Schema newsche = right.getSchema().joinWith(left.getSchema());
                            jn.setSchema(newsche);
                            int joinMeth = getJoinMethod(minRhsJoin, minLhsJoin);
                            jn.setJoinType(joinMeth);
                            String combinedName = getCombinedTablesName(minRhsJoin, minLhsJoin);
                            tab_op_hash.put(combinedName,jn);
                        }

                        break;
                    }
                    conditionIndex++;
                }
                System.out.println("conditionIndex is " + conditionIndex);

                // avoid cross product
                if(cn == null) continue;

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

    /** Generate all possible combination of choosing k distinct elements from joinTablesList **/
    private ArrayList<ArrayList<String>> getCombinations(ArrayList<String> sourceList, int k) {
        ArrayList<ArrayList<String>> result = new ArrayList<>();
        currentResult = new ArrayList<>();  // re-initialize global helper variable
        recursiveCombine(sourceList, result, 0, k);
        return result;
    }

    private void recursiveCombine(ArrayList<String> sourceList, ArrayList<ArrayList<String>> result,
                                  int offset, int sizeNeeded) {
        if (sizeNeeded == 0) {
            result.add(new ArrayList(currentResult));
            return;
        }

        for (int i=offset; i<=sourceList.size()-sizeNeeded; i++) {
            // choose or not choose
            currentResult.add(sourceList.get(i));
            recursiveCombine(sourceList, result, i + 1, sizeNeeded - 1);
            currentResult.remove(currentResult.size() - 1);
        }
    }



    /** partition combination to lhsplans and rhsplans, which form a plan*/
    private ArrayList<ArrayList<ArrayList<String>>> generatePlans(ArrayList<String> combination) {

        ArrayList<ArrayList<ArrayList<String>>> plans = new ArrayList<>();

        for (int i=1; i< combination.size(); i++) {
            ArrayList<ArrayList<String>> lhsPlans = getCombinations(combination, i);
            System.out.println("The number of lhs plans: ");
            System.out.println(lhsPlans.size());

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

    private int getJoinMethod(ArrayList<String> lhsPlan, ArrayList<String> rhsPlan) {

        String lhsPlanName = getCombinedTablesName(lhsPlan);
        String rhsPlanName = getCombinedTablesName(rhsPlan);

        int lhspages= 0;
        int rhspages = 0;

        for (Table table : jointTablesList) {
            if (table.getTableName().equals(lhsPlanName)) lhspages = (int) Math.ceil(table.getNumTuples()/Batch.getPageSize());
            if (table.getTableName().equals(rhsPlanName)) rhspages = (int) Math.ceil(table.getNumTuples()/Batch.getPageSize());
        }

        int blocksSize = (int) Math.floor(numBuffer/numJoin);

        double nestedJoin = lhspages*rhspages + lhspages;
        double blockNested = ((int) Math.ceil(lhspages/blocksSize)*rhspages) + lhspages;
        double hashJoin = 3*(lhspages + rhspages);

        Map<Integer, Double> costMap = new HashMap<>();
        costMap.put(0, nestedJoin);
        costMap.put(1, blockNested);
        costMap.put(2, hashJoin);

        Map<Integer, Double> sortedMap =
                costMap.entrySet().stream()
                        .sorted(Entry.comparingByValue())
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue,
                                (e1, e2) -> e1, LinkedHashMap::new));

        return (int) sortedMap.keySet().toArray()[0];
    }

    private double joinPlanCost(ArrayList<String> lhsPlan, ArrayList<String> rhsPlan) {
        // sub-plan must always inside the cost table
        // get the sub-combination cost, with <arrayList, double> pair
        double lhsPlanCost = costTable.get(lhsPlan);
        double rhsPlanCost = costTable.get(rhsPlan);

        String lhsPlanName = getCombinedTablesName(lhsPlan);
        System.out.println("lhs plan name : " + lhsPlanName);
        String rhsPlanName = getCombinedTablesName(rhsPlan);
        System.out.println("rhs plan name : " + rhsPlanName);

        int lhspages= 0;
        int rhspages = 0;

        for (Table table : jointTablesList) {
            if (table.getTableName().equals(lhsPlanName)) lhspages = (int) Math.ceil(table.getNumTuples()/Batch.getPageSize());
            if (table.getTableName().equals(rhsPlanName)) rhspages = (int) Math.ceil(table.getNumTuples()/Batch.getPageSize());
        }

        int blocksSize = 3;

        double nestedJoin = lhspages*rhspages + lhspages;
        double blockNested = ((int) Math.ceil(lhspages/blocksSize)*rhspages) + lhspages;
        double sortMerge = Double.MAX_VALUE;
        double hashJoin = 3*(lhspages + rhspages);

        ArrayList<Double> costArray = new ArrayList<Double>();
        costArray.add(nestedJoin);
        costArray.add(blockNested);
        costArray.add(hashJoin);
        costArray.add(sortMerge);
        Collections.sort(costArray);

        double totalcost = lhsPlanCost + rhsPlanCost + costArray.get(0);
        System.out.println("cost computed for this plan: " + totalcost);


        return totalcost;
    }

    /** return a set of table's attriute, which will contain information used in cost computation*/
    private ArrayList<Attribute> populateJointAttributesList(ArrayList<Attribute> jointAttributesList){

        for (int i = 0; i < joinlist.size(); i ++) {
            //costTable = new HashMap<ArrayList<String>, double>();
            Condition cn = (Condition) joinlist.elementAt(i);

            /** get attribute for left and right combo*/
            Attribute leftAttr = (Attribute) cn.getLhs();
            Attribute rightAttr = (Attribute) cn.getLhs();

            if(!jointAttributesList.contains(leftAttr)) jointAttributesList.add(leftAttr);
            if(!jointAttributesList.contains(rightAttr)) jointAttributesList.add(rightAttr);
        }

        return jointAttributesList;
    }


    private ArrayList<String> populateTableNameList(ArrayList<String> tableNameList) {

        for (Table table : jointTablesList) {
            tableNameList.add(table.getTableName());
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
                Table table = new Table(schm.getTupleSize(), tabname, schm);
                jointTablesList.add(table);
                _if.close();
            } catch (Exception e) {
                System.err.println("populateJointTableList:Error reading Schema of the table" + filename);
                System.exit(1);
            }
        }

        return jointTablesList;
    }

    private void setTupleNumber (ArrayList<Table> jointTablesList) {

        for(Table table : jointTablesList){  // For each table in from list

            String tabname = table.getTableName();
            String filename = tabname+".stat";

            try {
                File file = new File(filename);
                Scanner input = new Scanner(file);

                int index = 0;
                int numTuples = 0;
                int TUPLEINDEX = 0;

                while (input.hasNextLine()) {
                    String line = input.nextLine();
                    if(index == TUPLEINDEX) numTuples = Integer.parseInt(line);
                    index ++;
                }
                input.close();

                table.setNumTuples(numTuples);
                System.out.print("The number of tuples we get from the table " + table.getTableName());
                System.out.print(" is ");
                System.out.println(numTuples);

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

    }

    private String getCombinedTablesName(ArrayList<String> combination) {

        /** sort combinations so that for each combination, there is a unique name for min cost retrival*/
        Collections.sort(combination);

        String combinedName = "";
        for (String tab : combination) {
            combinedName = combinedName + tab;
        }

        return combinedName;
    }

    /** overloaded function*/
    private String getCombinedTablesName(ArrayList<String> combination1, ArrayList<String> combination2) {

        ArrayList<String> allCombo = new ArrayList<String>();
        allCombo.addAll(combination1);
        allCombo.addAll(combination2);

        /** Sort names to make the unique for a combination*/
        Collections.sort(allCombo);


        String newName = getCombinedTablesName(allCombo);

        return newName;
    }

    private Table joinTables(ArrayList<String> minLhsJoin, ArrayList<String> minRhsJoin) {

        ArrayList<String> allPlans = new ArrayList<String>();
        allPlans.addAll(minLhsJoin);
        allPlans.addAll(minRhsJoin);

        /** Sort names to make the unique for a combination*/
        Collections.sort(allPlans);


        // new table name
        String newTableName = getCombinedTablesName(allPlans);


        Table leftTab = null;
        Table rightTab = null;

        Collections.sort(minLhsJoin);
        Collections.sort(minRhsJoin);

        String lhsName = getCombinedTablesName(minLhsJoin);
        String rhsName = getCombinedTablesName(minRhsJoin);

        for (Table table : jointTablesList) {
            /** TODO: will it possible that the table is not in the list? */
            if (table.getTableName().equals(lhsName)) leftTab = table;
            if (table.getTableName().equals(rhsName)) rightTab = table;
        }

        //new schema
        Schema newSchema = leftTab.getSchema().joinWith(rightTab.getSchema());

        //new tuple number
        int newTupleNum = newSchema.getNumCols();

        return new Table(newTupleNum, newTableName, newSchema);
    }

    private HashMap<ArrayList<String>, Double> populateCostTable(HashMap<ArrayList<String>, Double> costTable) {

        /** initialize cost table with all tales from FROM list, and its cost as number of batches in the table**/
        for (int i = 0; i < jointTablesList.size(); i++) {
            ArrayList<String> tableCombo = new ArrayList<String>();
            tableCombo.add(jointTablesList.get(i).getTableName());
            System.out.println("The number of tuples: " + jointTablesList.get(i).getNumTuples());
            System.out.println("The tuple size is : " + jointTablesList.get(i).getSchema().getTupleSize());

            int numTuplesPerPage = (int) Math.ceil(Batch.getPageSize()/jointTablesList.get(i).getSchema().getTupleSize());
            System.out.println("The number of tuple per page : " + jointTablesList.get(i).getSchema().getTupleSize());
            double cost =jointTablesList.get(i).getNumTuples()/numTuplesPerPage;

            costTable.put(tableCombo, cost);
        }
        return costTable;
    }


    /**Table structure for cost computation*/
    class Table {
        int numTuples;
        String tableName;
        Schema schema;

        public Table(int numTuples, String tableName, Schema schema) {
            this.numTuples = numTuples;
            this.tableName = tableName;
            this.schema = schema;
        }

        public int getNumTuples() {
            return numTuples;
        }

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

    public void createProjectOp(){
        Operator base = root;
        System.out.println("root is: ");
        System.out.println(root);
        if ( projectlist == null )
            projectlist = new Vector();

        if(!projectlist.isEmpty()){
            root = new Project(base,projectlist,OpType.PROJECT);
            Schema newSchema = base.getSchema().subSchema(projectlist);
            root.setSchema(newSchema);
        }
    }
}



