package org.v5k.liquibase;

import liquibase.Liquibase;
import liquibase.change.ColumnConfig;
import liquibase.changelog.ChangeLogHistoryService;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.RanChangeSet;
import liquibase.database.Database;
import liquibase.database.core.PostgresDatabase;
import liquibase.datatype.core.ClobType;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.integration.spring.SpringLiquibase;
import liquibase.logging.LogService;
import liquibase.logging.LogType;
import liquibase.logging.Logger;
import liquibase.statement.core.AddColumnStatement;
import liquibase.statement.core.SelectFromDatabaseChangeLogStatement;
import liquibase.statement.core.UpdateStatement;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AutoRollbackSpringLiquibase extends SpringLiquibase {

    private static final String COLUMN_ID = "ID";
    private static final String COLUMN_AUTHOR = "AUTHOR";
    private static final String COLUMN_FILENAME = "FILENAME";
    private static final String COLUMN_ROLLBACK_SCRIPT = "ROLLBACK_SCRIPT";

    private Logger log = LogService.getLog(getClass());

    private String databaseCatalog;
    private RollbackParser rollbackParser;

    public AutoRollbackSpringLiquibase() {
        this.rollbackParser = new RollbackParser();
    }

    public String getDatabaseCatalog() {
        return databaseCatalog;
    }

    public void setDatabaseCatalog(String databaseCatalog) {
        this.databaseCatalog = databaseCatalog;
    }

    @Override
    protected void performUpdate(Liquibase liquibase) throws LiquibaseException {

        List<RanChangeSet> ranChangeSetList = liquibase.getDatabase().getRanChangeSetList();
        List<ChangeSet> changeSets = liquibase.getDatabaseChangeLog().getChangeSets();
        Database database = liquibase.getDatabase();
        Executor executor = ExecutorService.getInstance().getExecutor(liquibase.getDatabase());

        ChangeLogHistoryService changeLogHistoryService = ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database);
        changeLogHistoryService.init();

        SelectFromDatabaseChangeLogStatement select = new SelectFromDatabaseChangeLogStatement(new ColumnConfig().setName("*").setComputed(true));
        List<Map<String, ?>> changes = executor.queryForList(select);

        addRollbackScriptColumn(database, executor);
        Map<String, ?> rollbackMap = changes.stream().collect(Collectors.toMap(e -> key(e.get(COLUMN_FILENAME), e.get(COLUMN_ID), e.get(COLUMN_AUTHOR)), e -> e.get(COLUMN_ROLLBACK_SCRIPT)));

        for (int i = 0; i < ranChangeSetList.size(); i++) {
            RanChangeSet ranChangeSet = ranChangeSetList.get(i);
            if (i >= changeSets.size() || !isChangeStillRelevant(ranChangeSet, changeSets.get(i))) {
                String change = key(ranChangeSet.getChangeLog(), ranChangeSet.getId(), ranChangeSet.getAuthor());
                Object rollbackScript = rollbackMap.get(change);
                if (rollbackScript == null) {
                    throw new RuntimeException("Rollback script not found for change " + change);
                }
                rollback(liquibase, (String) rollbackScript);
            }
        }

        try {
            StringWriter rollbackWriter = new StringWriter();
            liquibase.futureRollbackSQL(rollbackWriter);
            List<Rollback> rollbacks = rollbackParser.parse(rollbackWriter.toString());
            super.performUpdate(liquibase);
            Collections.reverse(rollbacks);
            for (Rollback rollback : rollbacks) {
                UpdateStatement updateStatement = new UpdateStatement(getDatabaseCatalog(), getDatabaseSchema(), getDatabaseChangeLogTable());
                updateStatement.addNewColumnValue(COLUMN_ROLLBACK_SCRIPT, rollback.getScript());
                updateStatement.setWhereClause(String.format("%s='%s' AND %s='%s' AND %s='%s'", COLUMN_ID, rollback.getId(), COLUMN_AUTHOR, rollback.getAuthor(), COLUMN_FILENAME, rollback.getFilename()));
                executor.update(updateStatement);
                database.commit();
            }
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void addRollbackScriptColumn(Database database, Executor executor) throws DatabaseException {
        //TODO find a better way to verify whether column is present
        try {
            AddColumnStatement addColumnStatement = new AddColumnStatement(getDatabaseCatalog(), getDatabaseSchema(), getDatabaseChangeLogTable(), COLUMN_ROLLBACK_SCRIPT, new ClobType().toString(), null);
            executor.update(addColumnStatement);
            database.commit();
            log.debug(LogType.LOG, COLUMN_ROLLBACK_SCRIPT + " has been added to " + getDatabaseChangeLogTable());
        } catch (DatabaseException e) {
            //ignoring the exception if the column is already present // ugly way
            if (database instanceof PostgresDatabase) { // throws "current transaction is aborted" unless we roll back the connection
                database.rollback();
            }
        }
    }

    private static boolean isChangeStillRelevant(RanChangeSet ranChangeSet, ChangeSet changeSet) {
        return ranChangeSet.getLastCheckSum().equals(changeSet.generateCheckSum());
    }

    private String key(Object filename, Object id, Object author) {
        return filename + ":" + id + ":" + author;
    }

    private void rollback(Liquibase liquibase, String rollbackScript) throws LiquibaseException {
        try {
            File temp = File.createTempFile("temp", null);
            Files.write(temp.toPath(), rollbackScript.getBytes());
            liquibase.rollback(0, "file:" + temp.getAbsolutePath(), (String) null);
        } catch (IOException e) {
            throw new RuntimeException("Database migration failed", e);
        }
    }

    private String getDatabaseSchema() {
        return getLiquibaseSchema() != null ? getLiquibaseSchema() : getDefaultSchema();
    }

    @Override
    public String getDatabaseChangeLogTable() {
        return super.getDatabaseChangeLogTable() != null ? getDatabaseChangeLogTable() : "DATABASECHANGELOG";
    }
}
