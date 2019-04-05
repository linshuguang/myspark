package org.apache.spark.sql.catalyst.expressions.complexTypeCreator;

import lombok.Data;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParserUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/1/22.
 */
public class CreateNamedStruct extends  CreateNamedStructLike {
    List<Expression>children;

    public CreateNamedStruct(List<Expression>children){
        this.children = children;
    }

    List<Expression>nameExprs=null;
    List<Expression>valExprs=null;

    public List<Expression> nameExprs(){
        if(nameExprs==null){
            lazyVal();
        }
        return nameExprs;
    }

    public List<Expression> valExprs(){
        if(valExprs==null){
            lazyVal();
        }
        return valExprs;
    }

    private synchronized void lazyVal(){
        nameExprs = new ArrayList<>();
        valExprs = new ArrayList<>();
        for(int i =0, j=i+1; i<children.size() && j<children.size() ;i+=2){
            nameExprs.add(children.get(i));
            valExprs.add(children.get(j));
        }
    }

    @Override
    protected List<Expression>children(){
        return children;
    }


    @Override
    public boolean equals(Object o){
        if(o instanceof CreateNamedStruct){
            CreateNamedStruct c = (CreateNamedStruct)o;
            return ParserUtils.equalList(nameExprs(),c.nameExprs()) && ParserUtils.equalList(valExprs(),c.valExprs());
        }
        return false;
    }

}
