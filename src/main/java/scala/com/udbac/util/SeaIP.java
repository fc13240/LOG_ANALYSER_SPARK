package scala.com.udbac.util;

import java.util.List;

/**
 * Created by root on 2017/1/19.
 */
public class SeaIP {
    public static Integer searIP(List<Integer> rangeList, Integer ipInt) {
        int mid = rangeList.size() / 2;
        if (rangeList.get(mid) == ipInt) {
            return mid;
        }
        int start = 0;
        int end = rangeList.size() - 1;
        while (start <= end) {
            mid = (end - start) / 2 + start;
            if (ipInt < rangeList.get(mid)) {
                end = mid - 1;
                if(ipInt > rangeList.get(mid-1)){
                    return mid - 1;
                }
            } else if (ipInt > rangeList.get(mid)) {
                start = mid + 1;
                if (ipInt < rangeList.get(mid + 1)) {
                    return mid;
                }
            } else {
                return mid;
            }
        }
        return 0;
    }
}
