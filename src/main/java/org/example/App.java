package org.example;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;
import java.util.Scanner;

public class App
{
    private static final int limit = 10;

    public static void main( String[] args ) {

        Properties properties = new Properties();

        try (InputStream inputStream = App.class.getClassLoader().getResourceAsStream("config.properties")) {
            properties.load(inputStream);
        } catch (NullPointerException | IOException e) {
            System.err.println("Не удалось подключиться к config.properties");
            System.exit(1);
        }

        String url = properties.getProperty("jdbc.url");
        String username = properties.getProperty("jdbc.username");
        String password = properties.getProperty("jdbc.password");

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            System.out.println("Подключение установлено, введите SQL выражение (или QUIT для выхода):");

            Scanner scanner = new Scanner(System.in);
            while (true) {
                System.out.print("SQL> ");
                String sql = scanner.nextLine().trim();

                if (sql.equalsIgnoreCase("QUIT")) {
                    connection.close();
                    return;
                }
                if (sql.isEmpty()) {
                    continue;
                }

                try (Statement statement = connection.createStatement()) {
                    try (Statement countStmt = connection.createStatement();
                         ResultSet countRs = countStmt.executeQuery("SELECT COUNT(*) FROM (" + sql + ")")) {
                        countRs.next();
                        int totalRows = countRs.getInt(1);

                        statement.setMaxRows(limit);
                        statement.execute(sql);
                        ResultSet resultSet = statement.getResultSet();
                        while (resultSet.next()) {
                            ResultSetMetaData metaData = resultSet.getMetaData();
                            int columnCount = metaData.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                System.out.print(metaData.getColumnName(i) + ": " + resultSet.getString(i) + "\t");
                            }
                            System.out.println();
                        }
                        if (totalRows > 10) {
                            System.out.println("... (общее количество записей — " + totalRows + ")");
                        }
                    } catch (SQLException e) {
                        int updateCount = statement.executeUpdate(sql);
                        System.out.println("Успешно. Затронуто строк: " + updateCount);
                    }
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                    // System.err.println(e.getMessage()); //часто вылезает позже, чем следующий по итерации "SQL>"
                }
            }
        } catch (SQLException e) {
            System.err.println("Ошибка при подключении к БД: " + e.getMessage());
            System.exit(1);
        }
    }
}
