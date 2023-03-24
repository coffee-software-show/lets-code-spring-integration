package com.example.simpledbintegration;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.jdbc.JdbcMessageHandler;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.integration.jdbc.MessagePreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class SimpleDbIntegrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(SimpleDbIntegrationApplication.class, args);
    }

    @Bean
    JdbcPollingChannelAdapter jdbcPollingChannelAdapter(CustomerRowMapper customerRowMapper,
                                                        DataSource dataSource) {
        var jdbc = new JdbcPollingChannelAdapter(dataSource, "select * from CUSTOMER where processed = false");
        jdbc.setRowMapper(customerRowMapper);
      /*  jdbc.setUpdateSql("update customer set processed = true where id =  :id ");
        jdbc.setUpdatePerRow(true);
        jdbc.setUpdateSqlParameterSourceFactory(input -> {
            if (input instanceof Customer customer) {
                return new MapSqlParameterSource("id", customer.id());
            }
            System.out.println("the input is not a Customer");
            return null;
        });*/
        return jdbc;
    }

    @Bean
    JdbcMessageHandler jdbcMessageHandler(DataSource dataSource) {
        var update = new JdbcMessageHandler(dataSource, "update customer set processed = true where id = ?");
        update.setPreparedStatementSetter((ps, requestMessage) -> {
            var customer = (Customer) requestMessage.getPayload();
            ps.setInt(1, customer.id());
            ps.execute();
        });
        return update;
    }

    @Bean
    IntegrationFlow jdbcInboundFlow(JdbcPollingChannelAdapter inbound,
                                    JdbcMessageHandler  outbound
    ) {
        return IntegrationFlow
                .from(inbound, poller -> poller.poller(pm -> pm.fixedRate(1, TimeUnit.SECONDS)))
                .split()
                .handle((GenericHandler<Customer>) (payload, headers) -> {
                    System.out.println("-------");
                    System.out.println(payload);
                    headers.forEach((k, v) -> System.out.println(k + '=' + v));
                    return payload;
                })
                .handle(outbound)
                .get();
    }
}

@Component
class CustomerRowMapper implements RowMapper<Customer> {

    @Override
    public Customer mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new Customer(rs.getInt("id"), rs.getString("name"));
    }
}

record Customer(Integer id, String gname) {
}
