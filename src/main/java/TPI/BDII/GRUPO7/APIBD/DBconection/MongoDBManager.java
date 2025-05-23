package TPI.BDII.GRUPO7.APIBD.DBconection;

import TPI.BDII.GRUPO7.APIBD.Dtos.Casa;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;

import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;


public class MongoDBManager {

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> casasCollection;
    private MongoCollection<Document> movimientosCollection;

    // Constructor - Establece la conexión
    public MongoDBManager() {
        try {
            // Conectar a MongoDB (localhost:27017 por defecto)
            mongoClient = MongoClients.create("mongodb://localhost:27017");

            // Seleccionar la base de datos
            database = mongoClient.getDatabase("TPIBD");

            // Obtener las colecciones
            casasCollection = database.getCollection("casas");
            movimientosCollection = database.getCollection("movimientos");

            System.out.println("Conexión exitosa a MongoDB");

        } catch (Exception e) {
            System.err.println("Error al conectar con MongoDB");
        }
    }

    // === OPERACIONES CREATE (Crear documentos) ===

    public void crearCasa(Casa casa) {
        try {
            Document resultado = casasCollection.find()
                    .sort(Sorts.descending("_id"))
                    .first();

            int nuevoId = resultado != null ? resultado.getInteger("_id") + 1 : 1;

            Document insertCasa = new Document("_id", nuevoId)
                    .append("altura", casa.getAltura())
                    .append("sensores", casa.getSensores())
                    .append("costoMensual", casa.getCostoMensual());

            casasCollection.insertOne(insertCasa);
            System.out.println("Casa creada exitosamente con ID: " + nuevoId);

        } catch (Exception e) {
            System.err.println("Error al crear casa: " + e.getMessage());
        }
    }

    // === OPERACIONES READ (Consultar documentos) ===

    public List<Document> obtenerTodasLasCasas() {
        List<Document> retCasas = new ArrayList<>();
        try {
            FindIterable<Document> casas = casasCollection.find();

            for (Document casa : casas) {
                retCasas.add(casa);
            }

        } catch (Exception e) {
            System.err.println("Error al obtener casas");
        }
        return retCasas;
    }

    public Document obtenerCasasPorAltura(int altura) {
        Document retCasas = null;
        try {
            FindIterable<Document> casas = casasCollection.find(Filters.eq("altura", altura));

            retCasas = casas.first();

        } catch (Exception e) {
            System.err.println("Error al obtener casas por altura");
        }
        return retCasas;
    }

    public void cerrarConexion() { // para cerrar la conexión definitivamente
        if (mongoClient != null) {
            mongoClient.close();
            System.out.println("Conexión cerrada");
        }
    }
}
