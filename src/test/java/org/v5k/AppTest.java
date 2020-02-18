package org.v5k;

import liquibase.exception.LiquibaseException;
import org.h2.tools.Server;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.FileSystemResourceLoader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.v5k.config.DataSourceConfiguration;
import org.v5k.liquibase.AutoRollbackSpringLiquibase;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = ConfigFileApplicationContextInitializer.class)
@SpringBootTest(classes = {DataSourceConfiguration.class})
@TestPropertySource(locations = {"classpath:application.yml"})
public class AppTest {

    @Resource
    private DataSource dataSource;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Before
    public void initTest() throws SQLException {
        Server webServer = Server.createWebServer("-web", "-webAllowOthers", "-webPort", "8082");
        webServer.start();
    }

    @Test
    public void test() throws LiquibaseException {
        AutoRollbackSpringLiquibase liquibase = new AutoRollbackSpringLiquibase();
        liquibase.setDataSource(dataSource);
        liquibase.setChangeLog("classpath:db/changelog/changelog_v1.xml");
        liquibase.setResourceLoader(new FileSystemResourceLoader());
        liquibase.afterPropertiesSet();
        assertEquals(2, count());

        liquibase.setChangeLog("classpath:db/changelog/changelog_v2.xml");
        liquibase.afterPropertiesSet();
        assertEquals(3, count());

        liquibase.setChangeLog("classpath:db/changelog/changelog_v1.xml");
        liquibase.afterPropertiesSet();
        assertEquals(2, count());
    }

    private int count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM DATABASECHANGELOG", Integer.class);
    }
}
