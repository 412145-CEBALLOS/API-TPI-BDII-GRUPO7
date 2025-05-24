package TPI.BDII.GRUPO7.APIBD.DBconection;

import TPI.BDII.GRUPO7.APIBD.Dtos.CasaDTO;
import TPI.BDII.GRUPO7.APIBD.Dtos.EventoDTO;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;

import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.lang.Math.round;


public class MongoDBManager {

    private MongoClient mongoClient;
    private MongoCollection<Document> casasCollection;
    private MongoCollection<Document> eventosCollection;

    // Constructor - Establece la conexión
    public MongoDBManager() {
        try {
            // Conectar a MongoDB (localhost:27017 por defecto)
            mongoClient = MongoClients.create("mongodb://localhost:27017");

            // Seleccionar la base de datos
            MongoDatabase database = mongoClient.getDatabase("TPIBD");

            // Obtener las colecciones
            casasCollection = database.getCollection("casas");
            eventosCollection = database.getCollection("movimientos");

            System.out.println("Conexión exitosa a MongoDB");

        } catch (Exception e) {
            System.err.println("Error al conectar con MongoDB");
        }
    }

    // === OPERACIONES CREATE (Crear documentos) ===

    public void crearCasa(CasaDTO casaDTO) {
        try {
            Document resultado = casasCollection.find()
                    .sort(Sorts.descending("_id"))
                    .first();

            int nuevoId = resultado != null ? resultado.getInteger("_id") + 1 : 1;

            Document insertCasa = new Document("_id", nuevoId)
                    .append("altura", casaDTO.getAltura())
                    .append("sensores", casaDTO.getSensores())
                    .append("costoMensual", casaDTO.getCostoMensual());

            casasCollection.insertOne(insertCasa);
            System.out.println("Casa creada exitosamente con ID: " + nuevoId);

        } catch (Exception e) {
            System.err.println("Error al crear casa");
        }
    }

    public void crearEvento(EventoDTO eventoDTO) {
        try {
            Document resultado = eventosCollection.find()
                    .sort(Sorts.descending("_id"))
                    .first();

            int nuevoId = resultado != null ? resultado.getInteger("_id") + 1 : 1;

            Document insertEvento = new Document("_id", nuevoId)
                    .append("altura", eventoDTO.getAltura())
                    .append("sensor", eventoDTO.getSensor())
                    .append("fecha", eventoDTO.getFecha())
                    .append("hora", eventoDTO.getHora())
                    .append("segundos", eventoDTO.getSegundos());

            eventosCollection.insertOne(insertEvento);
            System.out.println("Casa creada exitosamente con ID: " + nuevoId);

        } catch (Exception e) {
            System.err.println("Error al crear evento");
        }
    }

    // === OPERACIONES READ (Consultar documentos) ===

    public List<Document> getAllCasas() {
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

    public List<Document> getAllEventos() {
        List<Document> retEventos = new ArrayList<>();
        try {
            FindIterable<Document> eventos = eventosCollection.find();

            for (Document evento : eventos) {
                retEventos.add(evento);
            }

        } catch (Exception e) {
            System.err.println("Error al obtener eventos");
        }
        return retEventos;
    }

    public Document getCasasByAltura(int altura) {
        Document retCasas = null;
        try {
            FindIterable<Document> casas = casasCollection.find(Filters.eq("altura", altura));

            retCasas = casas.first();

        } catch (Exception e) {
            System.err.println("Error al obtener casas por altura");
        }
        return retCasas;
    }

    public List<Document> getEventosByAltura(int altura) {
        List<Document> retEventos = new ArrayList<>();
        try {
            FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

            for (Document evento : eventos) {
                retEventos.add(evento);
            }

        } catch (Exception e) {
            System.err.println("Error al obtener eventos por altura");
        }
        return retEventos;
    }

    public String getHabitacionMasUsada(int altura) {
        Map<String, Integer> sensores = new HashMap<>();
        String sensorMasUsado = null;
        try {
            FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

            for (Document evento : eventos) {
                String sensor = evento.getString("sensor");
                sensores.put(sensor, sensores.getOrDefault(sensor, 0) + 1);
            }

            int maxValor = Integer.MIN_VALUE;

            for (Map.Entry<String, Integer> entry : sensores.entrySet()) {
                if (entry.getValue() > maxValor) {
                    maxValor = entry.getValue();
                    sensorMasUsado = entry.getKey();
                }
            }

        } catch (Exception e) {
            System.err.println("Error al obtener la habitacion mas frecuente");
        }
        return sensorMasUsado;
    }

    public Integer getTiempoPromedio(int altura, String sensor) {
        int tiempoAVG = 0;
        int cont = 0;
        try {
            FindIterable<Document> eventos = eventosCollection.find(
                    Filters.and(
                            Filters.eq("altura", altura),
                            Filters.eq("sensor", sensor)
                    )
            );

            for (Document evento : eventos) {
                tiempoAVG = tiempoAVG + evento.getInteger("segundos");
                cont++;
            }

        } catch (Exception e) {
            System.err.println("Error al obtener la habitacion mas frecuente");
        }
        return round((float) tiempoAVG / cont);
    }

    public String getHoraMasDetecciones(int altura) {
        Map<Integer, Integer> conteoPorHora = new HashMap<>();
        try {
            FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

            for (Document evento : eventos) {
                String horaCompleta = evento.getString("hora");
                int hora = Integer.parseInt(horaCompleta.split(":")[0]);
                conteoPorHora.put(hora, conteoPorHora.getOrDefault(hora, 0) + 1);
            }

            int maxHora = -1;
            int maxCount = -1;
            for (Map.Entry<Integer, Integer> entry : conteoPorHora.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxHora = entry.getKey();
                    maxCount = entry.getValue();
                }
            }

            return String.format("%02d:00", maxHora);

        } catch (Exception e) {
            System.err.println("Error al calcular la hora con más detecciones");
            return null;
        }
    }

    public String getUltimaDeteccionByAltura(int altura) {
        try {
            FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

            Document ultimaDeteccion = null;
            LocalDateTime fechaHoraMax = null;

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

            for (Document evento : eventos) {
                String fechaStr = evento.getString("fecha"); // formato "dd/MM/yyyy"
                String horaStr = evento.getString("hora");   // formato "HH:mm:ss"
                String fechaHoraStr = fechaStr + " " + horaStr;

                LocalDateTime fechaHora = LocalDateTime.parse(fechaHoraStr, formatter);

                if (fechaHoraMax == null || fechaHora.isAfter(fechaHoraMax)) {
                    fechaHoraMax = fechaHora;
                    ultimaDeteccion = evento;
                }
            }

            if (ultimaDeteccion != null) {
                String sensor = ultimaDeteccion.getString("sensor");
                return String.format("%s - %s", sensor, fechaHoraMax.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")));
            } else {
                return "No hay detecciones registradas";
            }

        } catch (Exception e) {
            System.err.println("Error al obtener la última detección: " + e.getMessage());
            return null;
        }
    }

    public List<String> getSensoresSinDeteccionMes(int altura) {
        List<String> sensoresSinDeteccion = new ArrayList<>();

        try {
            Document casa = getCasasByAltura(altura);

            // Obtener sensores esperados
            List<String> sensoresCasa = casa.getList("sensores", String.class);

            // Obtener mes y año actuales
            LocalDate hoy = LocalDate.now();
            int mesActual = hoy.getMonthValue();
            int anioActual = hoy.getYear();

            // Buscar eventos de este mes para esa altura
            FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

            Set<String> sensoresDetectadosEsteMes = new HashSet<>();
            DateTimeFormatter formatoFecha = DateTimeFormatter.ofPattern("dd/MM/yyyy");

            for (Document evento : eventos) {
                String fechaStr = evento.getString("fecha"); // ej. "28/10/2025"
                LocalDate fechaEvento = LocalDate.parse(fechaStr, formatoFecha);

                if (fechaEvento.getMonthValue() == mesActual && fechaEvento.getYear() == anioActual) {
                    sensoresDetectadosEsteMes.add(evento.getString("sensor"));
                }
            }

            // Comparar con sensores de la casa
            for (String sensor : sensoresCasa) {
                if (!sensoresDetectadosEsteMes.contains(sensor)) {
                    sensoresSinDeteccion.add(sensor);
                }
            }

        } catch (Exception e) {
            System.err.println("Error al consultar sensores sin detección: " + e.getMessage());
        }

        return sensoresSinDeteccion;
    }

    public void cerrarConexion() { // cerrar la conexión definitivamente
        if (mongoClient != null) {
            mongoClient.close();
            System.out.println("Conexión cerrada");
        }
    }
}

