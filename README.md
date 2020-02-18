Liquibase Auto Rollback
=
Introduction
-
The library will handle Liquibase auto rollbacks for you. Useful during active development when you need to deploy different versions of the application to environment
  

Alpha version. You can use it, but it's at your own risk 
-
How to use?
-
```
@Bean
public AutoRollbackLiquibase liquibase() {
    AutoRollbackLiquibase liquibase = new AutoRollbackLiquibase();
    liquibase.setChangeLog("classpath:changeLog.xml");
    liquibase.setDataSource(dataSource());
    return liquibase;
}
```