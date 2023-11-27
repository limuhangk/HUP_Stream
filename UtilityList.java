package ca.pfv.spmf.algorithms.frequentpatterns.HUP_Stream;

/* This file is copyright (c) 2018+  by Siddharth Dawar et al.
 *
 * This file is part of the SPMF DATA MINING SOFTWARE
 * (http://www.philippe-fournier-viger.com/spmf).
 *
 * SPMF is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with
 * SPMF. If not, see <http://www.gnu.org/licenses/>.
 */


import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a UtilityList as used by the HUI-Miner algorithm.
 *
 * @author Siddharth Dawar et al.
 * @see Element_CUL_List
 */
class UtilityList {
    /**
     * the item
     */
    int item;

    /**
     * the sum of item utilities
     */
    long sumIutils = 0;

    /**
     * the sum of remaining utilities
     */
    long sumRutils = 0;

    //    //两个标识符判断该项是否存在于最新批次或者最旧批次
    boolean isBelongToNewBatch = false;

    /**
     * the elements
     */
    List<Element_UtilityList> elements = new ArrayList<Element_UtilityList>();
    /**
     * Constructor.
     *
     * @param item the item that is used for this utility list
     */
    public UtilityList(int item) {
        this.item = item;
    }

    /**
     * Method to add an element to this utility list and update the sums at the same time.
     */
    public void addElement(Element_UtilityList element) {
        sumIutils += element.Nu;
        sumRutils += element.Nru;
        //Some conditions to update CU, CRU, and CPU
        elements.add(element);
    }


    //寻找tid所对应的element
    public boolean ifcontaintid(int tid) {
        if (findElementtotid(tid) != null)
            return true;
        return false;
    }

    /**
     * Method to remove an tid to this utility list and update the sums at the same time.
     */
    public void removetid(int tid) {
        Element_UtilityList element = findElementtotid(tid);
        sumIutils -= element.Nu;
        elements.remove(element);
    }

    public Element_UtilityList findElementtotid(int tid) {
//        CElement element = new CElement(tid, 0, 0, 0);
        int first = 0;
        int last = elements.size() - 1;
        while (first <= last) {
            int middle = (first + last) >>> 1; // divide by 2

            if (elements.get(middle).tid < tid) {
                first = middle + 1;  //  the itemset compared is larger than the subset according to the lexical order
            } else if (elements.get(middle).tid > tid) {
                last = middle - 1; //  the itemset compared is smaller than the subset  is smaller according to the lexical order
            } else {
                return elements.get(middle);
            }
        }
        return null;
    }

    public Element_UtilityList findElement(int tid) {
        for (Element_UtilityList element : elements) {
            if (element.tid == tid)
                return element;
        }
        return null;
    }
    public int getSupport() {
        return elements.size();
    }

}
