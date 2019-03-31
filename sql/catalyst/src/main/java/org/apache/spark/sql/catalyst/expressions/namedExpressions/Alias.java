package org.apache.spark.sql.catalyst.expressions.namedExpressions;


import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnaryExpression;
import org.apache.spark.sql.catalyst.parser.ParserUtils;
import org.apache.spark.sql.types.Metadata;

import javax.swing.text.html.parser.Parser;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kenya on 2019/1/30.
 */
@Data
public class Alias extends NamedExpression {
    String name;
    ExprId exprId;//:  = NamedExpression.newExprId,
    List<String> qualifier;//: Seq[String] = Seq.empty,
    Metadata explicitMetadata;//: Option[Metadata] = None

    public Alias(Expression child, String name){
        super(child);
        this.name = name;
        this.exprId = NamedExpression.newExprId();
        this.qualifier = new ArrayList<>();
        this.explicitMetadata = null;
    }

    @Override
    public boolean equals(Object other){
        //boolean ok = super.equals(other);

        if(other instanceof Alias) {
            Alias a = (Alias)other;

            return ParserUtils.equals(a.getChild(), getChild())
                    && StringUtils.equals(a.name,name)
                    && ParserUtils.equals(exprId,a.exprId)
                    && ParserUtils.equals(qualifier,a.qualifier)
                    && ParserUtils.equals(explicitMetadata,a.explicitMetadata);
        }
        return false;
    }

}
