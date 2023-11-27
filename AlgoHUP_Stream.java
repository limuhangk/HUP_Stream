package ca.pfv.spmf.algorithms.frequentpatterns.HUP_Stream;

import ca.pfv.spmf.tools.MemoryLogger;

import java.io.*;
import java.util.*;

public class AlgoHUP_Stream {
    //debug模式,debug=true，显示一些debug信息
    boolean debug = false;
    //最大内存使用率
    public double maxMemory = 0;
    //算法开始时间
    public long startTimestamp = 0;
    //临时变量计算时间
    public long tempTimestamp = 0;
    //某批次结束时间
    public long batchTimestamp = 0;
    //算法结束时间
    public long endTimestamp = 0;
    //计算处理旧批次所用时间
    public long oldBatchstartTimestamp = 0;
    public long oldBatchTimestamp = 0;
    //计算算法内存占用
    public long startMem = 0, endMem = 0;
    //算法当前处理的事务数
    public int transactionCount;
    //项与TWU的映射
    Map<Integer, Integer> mapItemToTWU;
    //项与效用列表的映射
    private Map<Integer, UtilityList> mapItemToUtilityList;
    //一项集效用列表的集合
    ArrayList<UtilityList> listOfUtilityLists;
    ArrayList<UtilityList> listULForRecursion;
    public HashMap<String, Eitemset> result_table = new HashMap<String, Eitemset>();
    //递归次数
    int recursive_calls = 0;
    int p_laprune = 0, p_cprune = 0;
    //生成的高效用项集的数量
    public int huiCount = 0;
    //候选项集数量
    public int candidateCount = 0;
    public int sumCandidateCount = 0;
    //所有事务的效用
    int totalDBUtility = 0;
    //最大事务数
    int MaxTid = 0;
    BufferedWriter writer = null;
    /**
     * the number of utility-list that was constructed
     */
    private int joinCount;

    /**
     * this class represent an item and its utility in a transaction
     */
    class Pair {
        int item = 0;
        int utility = 0;
    }

    /**
     * Write CHUIs found to a file. Note that this method write all CHUIs found
     * until now and erase the file by doing so, if the file already exists.
     *
     * @throws IOException if error writing to output file
     */
    public int writeHUIsToFile() throws IOException {
        int patternCount = 0;
        for (String itemset : result_table.keySet()) {
            long utility = result_table.get(itemset).sumUtility;
//            writer.write(convertToString(itemset, utility) + "\n");
            writer.write(itemset + "#util: " + utility + "\n");
            patternCount++;
        }
        return patternCount;
    }

    public String convertToString(int[] itemset, long utility) {
        // use a string buffer for more efficiency
        StringBuffer r = new StringBuffer();
        // for each item, append it to the stringbuffer
        for (int i = 0; i < itemset.length; i++) {
            r.append(itemset[i]);
            r.append(' ');
        }
        r.append("#util: ");
        r.append(utility);
//        r.append(" #sup: ");
//        r.append(support);
        return r.toString(); // return the tring
    }

    public String convertToString1(int[] itemset) {
        // use a string buffer for more efficiency
        StringBuffer r = new StringBuffer();
        // for each item, append it to the stringbuffer
        for (int i = 0; i < itemset.length; i++) {
            r.append(itemset[i]);
            r.append(' ');
        }
//        r.append(" #sup: ");
//        r.append(support);
        return r.toString(); // return the tring
    }
    //算法参数
    /**
     * number of batches that have been processed
     */
    int processedBatchCount;//记录处理的批次数量
    public int minutil;//最小效用阈值
    public int win_size, batch_size, win_number;//窗口大小，批次大小，窗口号
    private String resultFile;
    private List<Integer> TU = new ArrayList<Integer>();

    /**
     * Default constructor
     */
    public AlgoHUP_Stream() {
    }

    /**
     * Run the algorithm
     *
     * @param input      the input file path
     * @param resultFile the output file path
     * @param minutil    the minimum utility threshold
     * @throws IOException exception if error while writing the file
     */
    public void runAlgorithm(String input, int minutil, int win_size, int batch_size, String resultFile) throws IOException {
        // reset maximum
        MemoryLogger.getInstance().reset();
        Runtime r = Runtime.getRuntime();
        r.gc();//计算内存前先垃圾回收一次
        startTimestamp = System.currentTimeMillis();
        tempTimestamp = startTimestamp;
        startMem = r.freeMemory(); // 开始Memory
//        writer = new BufferedWriter(new FileWriter(resultFile));

        //  We create a  map to store the TWU of each item
        mapItemToTWU = new HashMap<Integer, Integer>();
        mapItemToUtilityList = new HashMap<Integer, UtilityList>();
        listOfUtilityLists = new ArrayList<>();
        BufferedReader myInput = null;
        String thisLine;
        int iterateBatch = 0, iterateWindow = 0;
        ArrayList<String> newBatchTransaction = new ArrayList<String>();//存储批次中的事务
        this.minutil = minutil;
        this.win_size = win_size;
        this.batch_size = batch_size;
        this.resultFile = resultFile;

        try {
            if (writer == null) {
                writer = new BufferedWriter(new FileWriter(resultFile));
            }
            // prepare the object for reading the file
            myInput = new BufferedReader(new InputStreamReader(
                    new FileInputStream(new File(input))));
            // for each line (transaction) until the end of file
            while ((thisLine = myInput.readLine()) != null) {
                // if the line is a comment, is empty or is a kind of metadata
                if (thisLine.isEmpty() == true || thisLine.charAt(0) == '#'
                        || thisLine.charAt(0) == '%'
                        || thisLine.charAt(0) == '@') {
                    continue;
                }
                iterateBatch++;//批次中事务的数量
                transactionCount++;
                //当前批次没有达到用户定义的一个批次中事务的数量，添加事务到批次当中
                if (iterateBatch <= this.batch_size) {
                    newBatchTransaction.add(thisLine);
                }
                //当前批次已达到用户定义的一个批次中事务的数量，将批次添加到窗口当中
                if ((iterateBatch == this.batch_size)) {
                    processedBatchCount++;//批次数++
                    iterateBatch = 0;//事务数归零
                    int oldestBatch = processedBatchCount - win_size <= 0 ? 1 : processedBatchCount - win_size;
                    if (processedBatchCount > win_size) {
                        oldBatchstartTimestamp = System.currentTimeMillis();
                        processOldBatch(oldestBatch);//处理闭合结果集中旧批次的项集
                        oldBatchTimestamp = System.currentTimeMillis() - oldBatchstartTimestamp;
                    }
                    miningProcessnewbatch(newBatchTransaction, processedBatchCount);//算法每次仅处理最新批次，不再区分是否是第一个窗口，进行统一处理
                    newBatchTransaction.clear();
                    batchTimestamp = System.currentTimeMillis() - tempTimestamp;
                    tempTimestamp = System.currentTimeMillis();
                    if (processedBatchCount >= win_size) {
                        int batchHuicount = writeHUIsToFile();
                        writeResultTofile(resultFile, processedBatchCount, batchHuicount, batchTimestamp, oldBatchTimestamp);
                        sumCandidateCount += candidateCount;
                        candidateCount = 0;
                        huiCount += batchHuicount;
                    }
                }
            }
            //两种异常情况，1、若事务数没有达到用户设定的批次中的数量，则批次数不会增加。2、若批次数没有达到窗口大小，则不会挖掘。
            // if it is last batch with elements less than user specified batch
            // elements
            if ((iterateBatch > 0) && (iterateBatch < this.batch_size)) {
                processedBatchCount++;
                int oldestBatch = processedBatchCount - win_size <= 0 ? 1 : processedBatchCount - win_size;
                if (processedBatchCount > win_size) {
                    oldBatchstartTimestamp = System.currentTimeMillis();
                    processOldBatch(oldestBatch);//处理闭合结果集中旧批次的项集
                    oldBatchTimestamp = System.currentTimeMillis() - oldBatchstartTimestamp;
                }
                miningProcessnewbatch(newBatchTransaction, processedBatchCount);//算法每次仅处理最新批次，不再区分是否是第一个窗口，进行统一处理
                transactionCount = (processedBatchCount - 1) * batch_size + 1;//当前处理的最新批次的第一个事务的事务Tid
                batchTimestamp = System.currentTimeMillis() - tempTimestamp;
                tempTimestamp = System.currentTimeMillis();
                if (processedBatchCount >= win_size) {
                    int batchHuicount = writeHUIsToFile();
                    writeResultTofile(resultFile, processedBatchCount, batchHuicount, batchTimestamp, oldBatchTimestamp);
                    sumCandidateCount += candidateCount;
                    candidateCount = 0;
                    huiCount += batchHuicount;
                }
                newBatchTransaction.clear();
            }
        } catch (Exception e) {
            // catches exception if error while reading the input file
            e.printStackTrace();
        } finally {
            if (myInput != null) {
                myInput.close();
                writer.close();
            }
            newBatchTransaction.clear();
        }
        endTimestamp = System.currentTimeMillis();
        endMem = r.freeMemory(); // 末尾Memory
    }

    //使用字典序升序
    private int compareItems(int item1, int item2) {
        int compare = mapItemToTWU.get(item1) - mapItemToTWU.get(item2);
//        int compare = item1 - item2;
//        return compare;
        // if the same, use the lexical order otherwise use the TWU
        return (compare == 0) ? item1 - item2 : compare;
    }

    //删除旧批次更新策略
    public void processOldBatch(int oldestBatch) {
        //更新所有项的TWU
        //删除效用列表中最旧批次所对应的tid及其元组
        int OldBatchTid = (oldestBatch - 1) * batch_size + 1;
        for (UtilityList ulist : listOfUtilityLists) {
            ulist.isBelongToNewBatch = false;
            if (ulist == null || ulist.elements.size() == 0)
                continue;
            while (ulist.elements.size() != 0 && ulist.elements.get(0).tid < OldBatchTid + batch_size) {
                Element_UtilityList elementFind = ulist.elements.get(0);
                ulist.sumIutils -= elementFind.Nu;// ulist.removetid(OldBatchTid + i);
                mapItemToTWU.put(ulist.item, mapItemToTWU.get(ulist.item) - TU.get(elementFind.tid - 1));
                ulist.elements.remove(elementFind);// ulist.removetid(OldBatchTid + i);
            }
        }

        Iterator iter = result_table.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String itemset = (String) entry.getKey();
            Eitemset bitemset = (Eitemset) entry.getValue();
            if (bitemset.Batch_elements.size() != 0 && bitemset.Batch_elements.get(0).bid == oldestBatch) {
                bitemset.sumUtility -= bitemset.Batch_elements.get(0).batch_util;
                bitemset.Batch_elements.remove(0);
                if (bitemset.sumUtility < minutil)
                    iter.remove();
            }
        }
    }

    //批次到达，仅处理最新批次
    //还是使用两次扫描数据库的方式，使用哈希表结构借助PPOS结构，合并相同事务的效用列表
    /*
     * para newBatchTransaction 当前处理的最新批次
     * para currentTid   当前处理的最新批次的第一个事务的Tid
     *
     * */
    public void miningProcessnewbatch(ArrayList<String> newBatchTransaction, int currentBatch) throws IOException {
        int tid = (currentBatch - 1) * batch_size + 1;
        //读取当前最新批次中的内容
        //计算项的TWU以及构架一项集的CUL-list
        for (String thisLine : newBatchTransaction) {//读取批次中的每个事务
            if (thisLine.isEmpty() == true
                    || thisLine.charAt(0) == '#' || thisLine.charAt(0) == '%' || thisLine.charAt(0) == '@') {
                continue;
            }
            // split the transaction according to the : separator
            String split[] = thisLine.split(":");
            // the first part is the list of items
            String items[] = split[0].split(" ");
            // the second part is the transaction utility
            int transactionUtility = Integer.parseInt(split[1]);
            // the third part is the items' utilies
            String utilityValues[] = split[2].split(" ");
            //在TU数组中保存事务的效用
            TU.add(transactionUtility);
            // for each item, we add the transaction utility to its TWU
            for (int i = 0; i < items.length; i++) {
                // convert item to integer
                Integer item = Integer.parseInt(items[i]);
                // get the current TWU of that item
                Integer twu = mapItemToTWU.get(item);
                // add the utility of the item in the current transaction to its twu
                UtilityList uList;
                if (twu == null) {
                    uList = new UtilityList(item);
                    mapItemToUtilityList.put(item, uList);
                    listOfUtilityLists.add(uList);
                    twu = transactionUtility;
                } else {
                    twu = twu + transactionUtility;
                    uList = mapItemToUtilityList.get(item);
                }
                mapItemToTWU.put(item, twu);
                // get the utility list of this item
                uList.addElement(new Element_UtilityList(tid, Integer.parseInt(utilityValues[i]), 0, 0, -1));
                uList.isBelongToNewBatch = true;
            }
            totalDBUtility += transactionUtility;
            tid++;
        }
        //若批次数大于窗口大小，窗口进行滑动
        if (currentBatch >= win_size) {
            listULForRecursion = new ArrayList<UtilityList>();
            for (UtilityList temp : listOfUtilityLists) {
                if (mapItemToTWU.get(temp.item) >= minutil && temp.isBelongToNewBatch == true && temp.sumIutils != 0) {
                    listULForRecursion.add(temp);
                }
            }
            // 按照字符顺序对效用列表进行排序
            Collections.sort(listULForRecursion, new Comparator<UtilityList>() {
                public int compare(UtilityList o1, UtilityList o2) {
                    // compare the TWU of the items
                    return compareItems(o1.item, o2.item);
                }
            });
            //倒序构建CUL-list，构建其剩余效用NRU 前缀效用PU 以及 PPos结构
            MaxTid = tid;
            int[] TA = new int[MaxTid + 1];//设置为事务数量大小
            int[] Position = new int[MaxTid + 1];//保存事务当中每个项之后项在其效用列表当中的位置
            Arrays.fill(TA, 0);
            Arrays.fill(Position, -1);
            //get the ul of item having largest TWU, then initialize the temp utility array TA
            UtilityList uli = (UtilityList) listULForRecursion.get(listULForRecursion.size() - 1);
            int pos = 0;
            for (Element_UtilityList ele : uli.elements) {
                ele.Nru = 0;
                ele.Ext = -1;
                TA[ele.tid] = ele.Nu;
                Position[ele.tid] = pos;
                pos++;
            }
            uli.sumRutils = 0;

            //re-calculate ru of smaller items
            ListIterator li = listULForRecursion.listIterator(listULForRecursion.size() - 1);
            while (li.hasPrevious()) {
                UtilityList ul = (UtilityList) li.previous();
                ul.sumRutils = 0;
                pos = 0;
                for (Element_UtilityList ele : ul.elements) {
                    ele.Nru = TA[ele.tid];
                    ul.sumRutils += TA[ele.tid];
                    TA[ele.tid] += ele.Nu;
                    ele.Ext = Position[ele.tid];
                    Position[ele.tid] = pos;
                    pos++;
                }
            }
            TA = null;
            Position = null;
            // Mine the database recursively
            System.out.println("效用列表当中项的数量为：" + listULForRecursion.size());
            Explore_search_tree(new int[0], listULForRecursion, minutil);
        }
        // check the memory usage
        checkMemory();
        // check the memory usage
        MemoryLogger.getInstance().checkMemory();
    }

    /**
     * This is the recursive method to find all high utility itemsets. It writes
     * the itemsets to the output file.
     *
     * @param prefix  This is the current prefix. Initially, it is empty.
     * @param ULs     The utility lists corresponding to each extension of the
     *                prefix.
     * @param minutil The minutil threshold.
     * @throws IOException
     */
    private void Explore_search_tree(int[] prefix, ArrayList<UtilityList> ULs,
                                     long minutil) throws IOException {
        recursive_calls++;
        // For each extension X of prefix P
        for (int i = 0; i < ULs.size(); i++) {
            UtilityList X = ULs.get(i);

            int[] sorted_prefix = new int[prefix.length + 1];
            System.arraycopy(prefix, 0, sorted_prefix, 0, prefix.length);
            sorted_prefix[prefix.length] = X.item;

            // If pX is a high utility itemset.
            // we save the itemset: pX
            if (X.sumIutils >= minutil) {
                writeOut(prefix, prefix.length, X, X.sumIutils);
            }
            candidateCount++;

            // If the sum of the remaining utilities for pX
            // is higher than minutil, we explore extensions of pX.
            // (this is the pruning condition)
            if (X.sumIutils + X.sumRutils >= minutil) {
                ArrayList<UtilityList> exULs = ConstructUL(X, ULs, i, minutil);
                Explore_search_tree(sorted_prefix, exULs, minutil);
            }
        }
        // check the maximum memory usage for statistics purpose
        MemoryLogger.getInstance().checkMemory();
    }

    /***
     * Construct a CUL
     * @param X
     * @param CULs
     * @param st
     * @param minutil
     * @return
     */

    private ArrayList<UtilityList> ConstructUL(UtilityList X, ArrayList<UtilityList> CULs, int st, long minutil) {
        ArrayList<UtilityList> exCULs = new ArrayList<UtilityList>();
        ArrayList<Long> LAU = new ArrayList<Long>();
        // Initialization
        for (int i = 0; i <= CULs.size() - 1; i++) {
            UtilityList uList = new UtilityList(CULs.get(i).item);
            exCULs.add(uList);
            LAU.add(X.sumIutils + X.sumRutils);  //LA-Prune
        }

        int extension = -1;
        for (Element_UtilityList ex : X.elements) {
            extension = ex.Ext;
            for (int j = st + 1; j <= CULs.size() - 1; j++) {
                List<Element_UtilityList> eylist = CULs.get(j).elements;
                if (exCULs.get(j) == null) {
                    if (extension < eylist.size() && extension != -1 && ex.tid == eylist.get(extension).tid)
                        extension = eylist.get(extension).Ext;
                    continue;
                }
                if (extension < eylist.size() && extension != -1 && ex.tid == eylist.get(extension).tid) {
                    UtilityList CULListOfItem = exCULs.get(j);
                    Element_UtilityList Y = eylist.get(extension);
                    Element_UtilityList element = new Element_UtilityList(ex.tid,
                            ex.Nu + Y.Nu - ex.Pu, Y.Nru, ex.Nu,
                            -1);
                    CULListOfItem.addElement(element);
//                    extension = eylist.get(extension).Ext;
                    extension = Y.Ext;
                } else // apply LA prune
                {
                    LAU.set(j, LAU.get(j) - ex.Nu - ex.Nru); // LA prune
                    if (LAU.get(j) < minutil) {
                        exCULs.set(j, null);
                        p_laprune++;
                    }
                }
            }
        }

        // filter
        ArrayList<UtilityList> filter_CULs = new ArrayList<UtilityList>();
        for (int j = st + 1; j <= CULs.size() - 1; j++) {
            if (exCULs.get(j) == null) {
                p_cprune++;
                continue;
            } else {
                filter_CULs.add(exCULs.get(j));
            }
        }
        if (filter_CULs.size() == 0)
            return filter_CULs;
        /**
         * 倒序更新filter_CULs
         * 改进的基于LA-Prune的剩余效用剪枝策略
         */
        int[] PositionNew = new int[MaxTid + 1];//保存事务当中每个项之后项在其效用列表当中的位置
        int[] newTA = new int[MaxTid + 1];
        Arrays.fill(PositionNew, -1);
        Arrays.fill(newTA, 0);
        UtilityList filculs = (UtilityList) filter_CULs.get(filter_CULs.size() - 1);
        int extension_new = 0;
        for (Element_UtilityList ele : filculs.elements) {
            ele.Nru = 0;
            newTA[ele.tid] = (ele.Nu - ele.Pu);
            ele.Ext = -1;
            PositionNew[ele.tid] = extension_new;
            extension_new++;
        }
        filculs.sumRutils = 0;

        ListIterator cul = filter_CULs.listIterator(filter_CULs.size() - 1);
        while (cul.hasPrevious()) {
            UtilityList ul1 = (UtilityList) cul.previous();
            ul1.sumRutils = 0;
            extension_new = 0;
            for (Element_UtilityList ele : ul1.elements) {
                ele.Nru = newTA[ele.tid];
                ul1.sumRutils += newTA[ele.tid];
                newTA[ele.tid] += (ele.Nu - ele.Pu);
                ele.Ext = PositionNew[ele.tid];
                PositionNew[ele.tid] = extension_new;
                extension_new++;
            }
        }
        PositionNew = null;
        newTA = null;
        return filter_CULs;
    }

    /**
     * Method to check the memory usage and keep the maximum memory usage.
     */
    private void checkMemory() {
        // get the current memory usage
        double currentMemory = (Runtime.getRuntime().totalMemory() - Runtime
                .getRuntime().freeMemory()) / 1024d / 1024d;
        // if higher than the maximum until now
        if (currentMemory > maxMemory) {
            // replace the maximum with the current memory usage
            maxMemory = currentMemory;
        }
    }

    private void writeOut(int[] prefix, int prefixLength, UtilityList utilityList, long utility) throws IOException {
        int[] itemset = new int[prefixLength + 1];
        itemset = appendItem(prefix, utilityList.item);
        quickSort(itemset, 0, itemset.length - 1);
//        Bitemset itemsetRetrieved = result_table.get(itemset);
        result_table.put(convertToString1(itemset), new Eitemset(utilityList, utility, batch_size));
    }

    /**
     * Append an item to an itemset
     *
     * @param itemset an itemset represented as an array of integers
     * @param item    the item to be appended
     * @return the resulting itemset as an array of integers
     */
    private int[] appendItem(int[] itemset, int item) {
        int[] newgen = new int[itemset.length + 1];
        System.arraycopy(itemset, 0, newgen, 0, itemset.length);
        newgen[itemset.length] = item;
        return newgen;
    }

    /**
     * quickSort
     *
     * @param arr
     * @param low
     * @param high
     */
    public void quickSort(int[] arr, int low, int high) {
        int i, j, temp, t;
        if (low > high) {
            return;
        }
        i = low;
        j = high;
        //temp就是基准位
        temp = arr[low];

        while (i < j) {
            //先看右边，依次往左递减
            while (temp <= arr[j] && i < j) {
                j--;
            }
            //再看左边，依次往右递增
            while (temp >= arr[i] && i < j) {
                i++;
            }
            //如果满足条件则交换
            if (i < j) {
                t = arr[j];
                arr[j] = arr[i];
                arr[i] = t;
            }

        }
        //最后将基准为与i和j相等位置的数字交换
        arr[low] = arr[i];
        arr[i] = temp;
        //递归调用左半数组
        quickSort(arr, low, j - 1);
        //递归调用右半数组
        quickSort(arr, j + 1, high);
    }

    /**
     * Write the result to a file
     *
     * @param path the output file path
     * @throws IOException if an exception for reading/writing to file
     */
    public void writeResultTofile(String path, int batchNumber, int batchHuicount, long time, long oldBatchtime) throws IOException {
        System.out.println("=============  Batch " + (batchNumber - win_size + 1) + " to " + batchNumber + " STATS =============");
        System.out.println("Time: " + time + " ms");
        System.out.println("Process old batch time: " + oldBatchtime + " ms");
        System.out.println("Memory: " + MemoryLogger.getInstance().getMaxMemory() + " MB");
        System.out.println("candidateCount: " + candidateCount);
        System.out.println("HUIs count: " + batchHuicount);
        System.out.println("================================================");
        writer.write("////////////////////////BATCH " + batchNumber);
        writer.newLine();
    }


    /**
     * Print statistics about the latest execution to System.out.
     */
    public void printStats() {
        System.out.println("=============  AlgoHUP_Stream ALGORITHM - STATS =============");
        System.out.println(" Minutil ~ "+ minutil);
        System.out.println(" Total time ~ " + (endTimestamp - startTimestamp) + " ms");
        System.out.println(" Memory ~ " + MemoryLogger.getInstance().getMaxMemory() + " MB");
        System.out.println(" High-utility itemsets count : " + huiCount);
        System.out.println(" candidateCount : " + sumCandidateCount);
        System.out.println("===================================================");
    }
}