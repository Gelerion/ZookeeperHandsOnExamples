package com.gelerion.zookeeper.sessions.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by denis.shuvalov on 11/04/2018.
 *
 * Auxiliary cache to handle changes to the lists of tasks and of workers.
 */
public class ChildrenCache {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected List<String> children;

    public ChildrenCache() {
        this.children = null;
    }

    public ChildrenCache(List<String> children) {
        this.children = children;
    }

    public List<String> getList() {
        return children;
    }

    public List<String> addedAndSet( List<String> newChildren) {
        ArrayList<String> diff = null;

        if (children == null) {
            diff = new ArrayList<>(newChildren);
        }
        else {
            for (var newChild : newChildren) {
                if (!children.contains(newChild)) {
                    if(diff == null) diff = new ArrayList<>();
                    diff.add(newChild);
                }
            }
        }

        this.children = newChildren;
        return diff;
    }

    public List<String> removedAndSet(List<String> newChildren) {
        List<String> diff = null;

        if(children != null) {
            for(var existingChild: children) {
                if(!newChildren.contains(existingChild)) {
                    if(diff == null) diff = new ArrayList<>();
                    diff.add(existingChild);
                }
            }
        }
        this.children = newChildren;
        return diff;
    }

}
