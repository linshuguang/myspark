package org.apache.spark.sql.catalyst.plans.logical.basicLogicalOperators;

import org.apache.spark.sql.catalyst.expressions.namedExpressions.NamedExpression;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.OrderPreservingUnaryNode;

import java.util.List;

/**
 * Created by kenya on 2019/2/15.
 */
public class Project extends OrderPreservingUnaryNode {
    List<NamedExpression> projectList;

    public Project(List<NamedExpression> projectList, LogicalPlan child){
        super(child);
        this.projectList = projectList;
    }

    private boolean empyList(List<NamedExpression> list){
        return list==null && list.size()==0;
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof Project){
            Project p = (Project)o;

            boolean b1=empyList(projectList);
            boolean b2=empyList(p.projectList);
            if(b1 && b2){
                return true;
            }
            if((!b1 && b2 )||(b1 && !b2)){
                return false;
            }
            if(projectList.size()!=p.projectList.size()){
                return false;
            }
            for(int i=0;i<projectList.size();i++){
                if(!ParserUtils.equals(projectList.get(i),p.projectList.get(i))){
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
