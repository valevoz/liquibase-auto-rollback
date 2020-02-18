package org.v5k.liquibase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RollbackParser {

    private static final String START = "-- Rolling Back ChangeSet:";
    private static final String END = "-- Release Database Lock";

    public List<Rollback> parse(String sql) {
        String[] blocks = sql.split(START);
        if (blocks.length < 2) {
            return Collections.emptyList();
        }
        List<Rollback> rollbacks = new ArrayList<>();
        for (int i = 1; i < blocks.length - 1; i++) {
            Rollback rollback = createRollback(blocks[i]);
            rollbacks.add(rollback);
        }
        rollbacks.add(createRollback(blocks[blocks.length - 1].split(END)[0]));
        return rollbacks;
    }

    private Rollback createRollback(String block) {
        String[] rollbackInfo = block.split("\n", 2);
        String[] changeInfo = rollbackInfo[0].split("::");
        return new Rollback(changeInfo[1].trim(), changeInfo[2].trim(), changeInfo[0].trim(), rollbackInfo[1].trim());
    }
}
