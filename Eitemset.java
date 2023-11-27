package ca.pfv.spmf.algorithms.frequentpatterns.HUP_Stream;


import java.util.ArrayList;
import java.util.List;

public class Eitemset {
    long OldestBatchUtility;
    long sumUtility;
    public List<Batch_tuple> Batch_elements;

    public Eitemset(long utility, long OldestBatchUtility){
        this.sumUtility = utility;
        this.OldestBatchUtility = OldestBatchUtility;
    }

    public Eitemset(UtilityList culList, long utility, int batchsize) {
        this.sumUtility = utility;
        Batch_elements = new ArrayList<Batch_tuple>();
        Batch_tuple temp;
        int tuple_count = 0;
        int batch_temp = 0;
        for (Element_UtilityList element : culList.elements) {
            int batch_number = 0;
            if (element.tid % batchsize == 0)
                batch_number = element.tid / batchsize;
            else
                batch_number = element.tid / batchsize + 1;
            //判断当前element是否属于已插入的批次，不是的话就插入，是的话更新
            if (batch_temp != batch_number) {
                batch_temp = batch_number;
                temp = new Batch_tuple(batch_number, element.Nu);
                Batch_elements.add(tuple_count, temp);
                tuple_count++;
            } else {
                temp = Batch_elements.get(tuple_count - 1);
                temp.batch_util += element.Nu;
            }
        }
    }

}
