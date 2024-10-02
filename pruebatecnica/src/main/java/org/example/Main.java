package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.sql.*;
import java.time.LocalDate;

public class Main {
    public static void main(String[] args) {

        int intentos = 3;
        int milisegundos = 3600000;

        for(int i=1; i<=intentos; i++){

            System.out.println("Intento N°" + i);

            try{
                cargarDatos();
                break;
            }
            catch (Exception e){
                if(i==3){
                    System.out.println("Se han acabado los intentos. El programa se cerrará");
                }
                else {
                    System.out.println("Ha ocurrido un error. Se esperará 1 hora para el siguiente intento.");
                    try {
                        Thread.sleep(milisegundos);
                    } catch (Exception ex) {
                        Thread.currentThread().interrupt();
                        System.out.println("Error durante el intervalo de espera");
                    }
                }
            }

        }


    }


    public static Boolean verificarCargaDiaria() throws Exception{

        String query = "SELECT * FROM caerp17 WHERE Fecha_disp = ? LIMIT 1";
        LocalDate fechaHoy = LocalDate.now();

        try {
            Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/datawarehouse", "root", "root");
            PreparedStatement preparedStatement = connection.prepareStatement(query);

                // Establecer el parámetro de la consulta
                preparedStatement.setObject(1, fechaHoy);

                // Ejecutar la consulta
                ResultSet resultSet = preparedStatement.executeQuery();

                // Verificar si hay resultados
                return resultSet.next();

        }
        catch (SQLException e){
            throw e;
        }


    }


    public static void cargarDatos() throws Exception{

        String urlJson = "../JSON listado.json";

        ObjectMapper mapper = new ObjectMapper();

        if(verificarCargaDiaria()){
            System.out.println("Ya se ha hecho una carga el día de hoy");
            return;
        }

        try {

            JsonNode lectorJson = mapper.readTree(new File(urlJson));

            String query = "INSERT INTO caerp17 (`rubro.nombre`, `empresa.cliente.nombre`, Fecha_disp) VALUES (?,?,?)";
            LocalDate fechaHoy = LocalDate.now();

            try{
                Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/datawarehouse", "root", "root");

                PreparedStatement preparedStatement = connection.prepareStatement(query);

                for(JsonNode item : lectorJson){

                    preparedStatement.setString(1, item.path("rubro").path("nombre").asText());
                    preparedStatement.setString(2, item.path("empresa").path("cliente").path("nombre").asText());
                    preparedStatement.setObject(3, fechaHoy);

                    preparedStatement.executeUpdate();

                }

                System.out.println("Datos insertados correctamente");

            }
            catch (Exception e){
                throw e;
            }

        }
        catch (Exception e){
            throw e;
        }

    }


}