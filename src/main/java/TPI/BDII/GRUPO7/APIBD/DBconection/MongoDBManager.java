package TPI.BDII.GRUPO7.APIBD.DBconection;

import TPI.BDII.GRUPO7.APIBD.Dtos.CasaDTO;
import TPI.BDII.GRUPO7.APIBD.Dtos.ConsumoDTO;
import TPI.BDII.GRUPO7.APIBD.Dtos.EventoDTO;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;

import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
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
                    .append("consumo", convertirConsumoDocument(casaDTO.getConsumoDTO()))
                    .append("costoMensual", casaDTO.getCostoMensual());


            casasCollection.insertOne(insertCasa);
            System.out.println("Casa creada exitosamente con ID: " + nuevoId);

        } catch (Exception e) {
            System.err.println("Error al crear casa");
        }
    }

    private Document convertirConsumoDocument(ConsumoDTO consumoDTO) {
        return new Document("escalon", consumoDTO.getEscalon())
                .append("bonificacion", consumoDTO.getBonificacion())
                .append("alerta", consumoDTO.getAlerta());
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
        return cont > 0 ? Math.round((float) tiempoAVG / cont) : 0;
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



    // En algunos casos no se si la forma de respuesta sea la optima
    // por eso decia que queria ir probando por ensayo y error si lograba hacer funcionar las estadisticas


    public Document getConsumoCasaHabitacion(Integer altura) {
        // Filtrar eventos por altura
        FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

        // Contar eventos por habitación
        Map<String, Integer> eventosPorHabitacion = new HashMap<>();
        for (Document evento : eventos) {
            String habitacion = evento.getString("sensor");
            eventosPorHabitacion.put(habitacion, eventosPorHabitacion.getOrDefault(habitacion, 0) + 1);
        }

        // Calcular consumo por habitación (puse para que sume 10 unidades de consumo por evento, cambiar si se quiere otro numeor)
        List<Document> habitaciones = new ArrayList<>();
        int consumoTotal = 0;
        for (Map.Entry<String, Integer> entry : eventosPorHabitacion.entrySet()) {
            int consumo = entry.getValue() * 10;
            Document docHabitacion = new Document()
                    .append("habitacion", entry.getKey())
                    .append("consumo", consumo);
            habitaciones.add(docHabitacion);
            consumoTotal += consumo;
        }


        Document resumen = new Document();
        resumen.append("casaId", altura);
        resumen.append("consumoTotal", consumoTotal);
        resumen.append("porHabitacion", habitaciones);

        return resumen;
    }


    public List<Document> getTop3CasasMayorConsumo() {
        // Traer todos los eventos
        FindIterable<Document> eventos = eventosCollection.find();

        // Contar eventos por casa
        Map<Integer, Integer> eventosPorCasa = new HashMap<>();
        for (Document evento : eventos) {
            Integer altura = evento.getInteger("altura");
            eventosPorCasa.put(altura, eventosPorCasa.getOrDefault(altura, 0) + 1);
        }

        // Crear lista con consumo total (* 10)
        List<Document> listaConsumo = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : eventosPorCasa.entrySet()) {
            int consumo = entry.getValue() * 10;
            listaConsumo.add(new Document()
                    .append("casaId", entry.getKey())
                    .append("consumoTotal", consumo));
        }

        // Ordenar de forma descendente
        listaConsumo.sort((d1, d2) -> d2.getInteger("consumoTotal").compareTo(d1.getInteger("consumoTotal")));

        // Devolver los primeros 3 elementos
        return listaConsumo.subList(0, Math.min(3, listaConsumo.size()));
    }




    public List<Document> getConsumoPorDia(Integer altura) {
        // Formato de fecha y hora
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

        // Filtrar eventos por casa
        FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

        // Uso treemap para que se ordenen los dias
        Map<Integer, Integer> eventosPorDiaSemana = new TreeMap<>();

        //Recorrer eventos
        for (Document evento : eventos) {
            String fechaStr = evento.getString("fecha");
            String horaStr = evento.getString("hora");

            // Unir fecha y hora y convertir
            LocalDateTime fechaHora = LocalDateTime.parse(fechaStr + " " + horaStr, formatter);

            // Obtener el dia
            int diaSemana = fechaHora.getDayOfWeek().getValue();

            // Contar eventos por dia
            eventosPorDiaSemana.put(diaSemana, eventosPorDiaSemana.getOrDefault(diaSemana, 0) + 10);
        }

        // Crear lista con los dias y cantidad de eventos
        List<Document> listaDias = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : eventosPorDiaSemana.entrySet()) {
            listaDias.add(new Document()
                    .append("dia", entry.getKey())
                    .append("consumoDia", entry.getValue()));
        }


        return listaDias;
    }




    public List<Document> getConsumoHora(Integer altura) {
        // Formato de fecha y hora
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

        // Buscar eventos por casa
        FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

        // contar eventos por hora
        Map<Integer, Integer> eventosPorHora = new TreeMap<>();

        for (Document evento : eventos) {
            String fechaStr = evento.getString("fecha");
            String horaStr = evento.getString("hora");

            LocalDateTime fechaHora = LocalDateTime.parse(fechaStr + " " + horaStr, formatter);

            // Obtener la hora
            int hora = fechaHora.getHour();

            // Sumar evento para la hora
            eventosPorHora.put(hora, eventosPorHora.getOrDefault(hora, 0) + 1);
        }

        // Armar lista de hora y consumo (* 10)
        List<Document> consumoPorHora = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : eventosPorHora.entrySet()) {
            consumoPorHora.add(new Document()
                    .append("hora", entry.getKey())
                    .append("consumo", entry.getValue() * 10));
        }

        return consumoPorHora;
    }

    public double getCostoEstimadoMensual(Integer altura, int mes, int anio) {
        //Si se quiere hacer obtener la tarifa de otra forma entonces se agrega como parametro
        double tarifa = 10;
        // Formato fecha + hora
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

        // Filtrar everntos por casa
        FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

        int totalEventosDelMes = 0;

        for (Document evento : eventos) {
            String fechaStr = evento.getString("fecha");
            String horaStr = evento.getString("hora");

            LocalDateTime fechaHora = LocalDateTime.parse(fechaStr + " " + horaStr, formatter);

            // Filtrar por mes y año
            if (fechaHora.getMonthValue() == mes && fechaHora.getYear() == anio) {
                totalEventosDelMes++;
            }
        }

        // Calcular consumo total
        double consumo = totalEventosDelMes * 10;

        // Calcular costo
        return consumo * tarifa;
    }



    public List<Document> getConsumoPorDiaSemanaYMes(Integer altura) {
        // Formato fecha - hora
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

        // Filtrar por casa
        FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

        // mapas para mantener el orden
        Map<String, Integer> consumoPorDia = new TreeMap<>();
        Map<String, Integer> consumoPorSemana = new TreeMap<>();
        Map<String, Integer> consumoPorMes = new TreeMap<>();

        for (Document evento : eventos) {
            String fechaStr = evento.getString("fecha");
            String horaStr = evento.getString("hora");

            LocalDateTime fechaHora = LocalDateTime.parse(fechaStr + " " + horaStr, formatter);

            // Suma para día
            String dia = fechaHora.format(DateTimeFormatter.ofPattern("dd/MM/yyyy"));
            consumoPorDia.put(dia, consumoPorDia.getOrDefault(dia, 0) + 10);

            // Suma para semana
            WeekFields weekFields = WeekFields.of(Locale.getDefault());
            int semana = fechaHora.get(weekFields.weekOfWeekBasedYear());
            int anio = fechaHora.getYear();
            String semanaKey = String.format("%04d-%02d", anio, semana);
            consumoPorSemana.put(semanaKey, consumoPorSemana.getOrDefault(semanaKey, 0) + 10);

            // Suma para mes
            String mes = fechaHora.format(DateTimeFormatter.ofPattern("MM/yyyy"));
            consumoPorMes.put(mes, consumoPorMes.getOrDefault(mes, 0) + 10);
        }

        // Convertir mapas a lista
        List<Document> resultado = new ArrayList<>();

        for (Map.Entry<String, Integer> entry : consumoPorDia.entrySet()) {
            resultado.add(new Document()
                    .append("periodo", entry.getKey())
                    .append("tipo", "dia")
                    .append("consumo", entry.getValue()));
        }

        for (Map.Entry<String, Integer> entry : consumoPorSemana.entrySet()) {
            resultado.add(new Document()
                    .append("periodo", entry.getKey())
                    .append("tipo", "semana")
                    .append("consumo", entry.getValue()));
        }

        for (Map.Entry<String, Integer> entry : consumoPorMes.entrySet()) {
            resultado.add(new Document()
                    .append("periodo", entry.getKey())
                    .append("tipo", "mes")
                    .append("consumo", entry.getValue()));
        }

        return resultado;
    }













    public void cerrarConexion() { // cerrar la conexión definitivamente
        if (mongoClient != null) {
            mongoClient.close();
            System.out.println("Conexión cerrada");
        }
    }
}

