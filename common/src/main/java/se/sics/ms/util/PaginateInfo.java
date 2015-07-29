package se.sics.ms.util;

/**
 * Helper class containing the information
 * for pagination.
 * Created by babbar on 2015-07-15.
 */
public class PaginateInfo {

    private int from;
    private int size;


    public PaginateInfo(int from, int size){
        this.from = from;
        this.size = size;
    }

    @Override
    public String toString() {
        return "PaginateInfo{" +
                "from=" + from +
                ", size=" + size +
                '}';
    }

    public int getSize(){
        return this.size;
    }

    public int getFrom(){
        return this.from;
    }


}
