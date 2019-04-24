package org.apache.spark.sql.catalyst.util;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Created by kenya on 2019/4/8.
 */
public class  BoundedPriorityQueue<A> implements Iterable<A>,Serializable {

    PriorityQueue underlying;
    int maxSize;
    Comparator<A> comparator;

    public BoundedPriorityQueue(int maxSize, Comparator<A> comparator){
        this.maxSize = maxSize;
        this.comparator = comparator;
        underlying = new PriorityQueue<A>(maxSize,comparator);
    }

    @Override
    public Iterator<A> iterator(){
        return underlying.iterator();
    }

    int size(){
      return underlying.size();
    }


    public BoundedPriorityQueue<A> assign(A elem){
        if (size() < maxSize) {
            underlying.offer(elem);
        } else {
            maybeReplaceLowest(elem);
        }
        return this;
    }

    private boolean maybeReplaceLowest(A a){
        Object head = underlying.peek();
        if (head != null && comparator.compare(a, (A)head)>0) {
            underlying.poll();
            return underlying.offer(a);
        } else {
            return false;
        }
    }



}
