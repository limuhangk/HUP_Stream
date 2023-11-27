package ca.pfv.spmf.algorithms.frequentpatterns.HUP_Stream;

/**
 * 构建 结果集中项集的每个批次的元组，该元组由批次号bid以及批次效用batch_util两部分组成
 */
public class Batch_tuple {
    int bid;
    long batch_util;

    public Batch_tuple(int bid, long batch_util) {
        this.bid = bid;
        this.batch_util = batch_util;
    }

}
