package wmy.hadoop.mapreduce.quickSort;

/*
 *@description: 实现快速排序
 *@author: 情深@骚明
 *@time: 2021/2/5 8:07
 *@Version 1.0
 */

/**
 * 利用分治算法和递归的思想来进行快速排序
 * 快速排序之所比较快，因为相比冒泡排序，每次交换是跳跃式的。每次排序的时候设置一个基准点，
 * 将小于等于基准点的数全部放到基准点的左边，将大于等于基准点的数全部放到基准点的右边。
 * 这样在每次交换的时候就不会像冒泡排序一样每次只能在相邻的数之间进行交换，交换的距离
 * 就大的多了。因此总的比较和交换次数就少了，速度自然就提高了。当然在最坏的情况下，
 * 仍可能是相邻的两个数进行了交换。因此快速排序的最差时间复杂度和冒泡排序是一样的都是O(N2)，
 * 它的平均时间复杂度为O(NlogN)。
 */
public class quickSort {
    public static void sort(int[] arr, int low, int high) throws Exception {
        if (low > high) throw(new Exception("传入的参数出现错误：low > high"));

        int i = low; // 左哨兵
        int j = high; // 右哨兵
        int tmp = arr[low]; // 基准值

        while (i < j) { // 循环比较的条件
            while (tmp <= arr[j] && i < j) {  // 右边的数据统一比左边的数据大
                j--;
            }
            while (tmp >= arr[i] && i < j) { // 左边的数据统一比左边的要大
                i++;
            }
            if (i < j) {
                int temp = arr[j];
                arr[j] = arr[i];
                arr[i] = temp;
            }
        }
        // 交换基准值
        arr[low] = arr[i];
        arr[i] = tmp;

        // 递归查询
        sort(arr, low, j - 1);
        sort(arr, j + 1, high);
    }

    public static void main(String[] args) throws Exception {
        // 测试数据
        int[] arr = {6, 1, 2, 7, 9, 3, 4, 5, 10, 8};
        sort(arr, 0, arr.length - 1);
        for (int i = 0; i < arr.length; i++) {
            System.out.println(arr[i]);
        }
    }
}