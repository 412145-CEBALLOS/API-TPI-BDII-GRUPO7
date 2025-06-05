package TPI.BDII.GRUPO7.APIBD.DBconection;

import TPI.BDII.GRUPO7.APIBD.Dtos.CasaDTO;
import TPI.BDII.GRUPO7.APIBD.Dtos.ConsumoDTO;
import TPI.BDII.GRUPO7.APIBD.Dtos.EventoDTO;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;

import com.mongodb.client.model.Sorts;
import org.bson.Document;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.WeekFields;
import java.util.*;



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

    public List<Document> getEventosRangoFechas(int altura, String fechaInicio, String fechaFin) {
        List<Document> eventosDetectados = new ArrayList<>();

        try {
            // Parsear fechas de entrada
            DateTimeFormatter formatoFecha = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            LocalDate fechaInicioLD = LocalDate.parse(fechaInicio, formatoFecha);
            LocalDate fechaFinLD = LocalDate.parse(fechaFin, formatoFecha);

            // Buscar eventos en el rango de fechas para esa altura
            FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

            for (Document evento : eventos) {
                String fechaStr = evento.getString("fecha"); // ej. "15/02/2025"
                LocalDate fechaEvento = LocalDate.parse(fechaStr, formatoFecha);

                // Verificar si la fecha del evento está dentro del rango (inclusive)
                if ((fechaEvento.isEqual(fechaInicioLD) || fechaEvento.isAfter(fechaInicioLD)) &&
                        (fechaEvento.isEqual(fechaFinLD) || fechaEvento.isBefore(fechaFinLD))) {
                    eventosDetectados.add(evento);
                }
            }

        } catch (DateTimeParseException e) {
            System.err.println("Error al parsear las fechas. Formato esperado: dd/MM/yyyy - " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error al consultar eventos: " + e.getMessage());
        }

        return eventosDetectados;
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
        FindIterable<Document> eventos = eventosCollection.find();

        Map<Integer, Integer> eventosPorCasa = new HashMap<>();
        for (Document evento : eventos) {
            Integer altura = evento.getInteger("altura");
            eventosPorCasa.put(altura, eventosPorCasa.getOrDefault(altura, 0) + 1);
        }

        List<Document> listaConsumo = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : eventosPorCasa.entrySet()) {
            int consumo = entry.getValue() * 10;
            listaConsumo.add(new Document()
                    .append("casa", entry.getKey())
                    .append("consumo", consumo));
        }

        listaConsumo.sort((d1, d2) -> d2.getInteger("consumo").compareTo(d1.getInteger("consumo")));

        return listaConsumo.subList(0, Math.min(3, listaConsumo.size()));
    }


    // Ejemplo de formato: "http://localhost:8080/api/v1/consumo-dia?altura=199&fecha=02/03/2025"
    public List<Document> getConsumoPorDia(Integer altura, String fechaInput) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

        LocalDate fechaFiltrada;
        try {
            fechaFiltrada = LocalDate.parse(fechaInput, DateTimeFormatter.ofPattern("dd/MM/yyyy"));
        } catch (DateTimeParseException e) {
            return Collections.emptyList();
        }

        FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));
        Map<String, Integer> eventosPorFecha = new TreeMap<>();

        for (Document evento : eventos) {
            String fechaStr = evento.getString("fecha");
            String horaStr = evento.getString("hora");

            if (fechaStr == null || horaStr == null) continue;

            try {
                LocalDateTime fechaHora = LocalDateTime.parse(fechaStr + " " + horaStr, formatter);
                if (fechaHora.toLocalDate().equals(fechaFiltrada)) {
                    String fechaSolo = fechaHora.toLocalDate().toString();
                    eventosPorFecha.put(fechaSolo, eventosPorFecha.getOrDefault(fechaSolo, 0) + 1);
                }
            } catch (DateTimeParseException e) {
            }
        }

        List<Document> consumoPorDia = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : eventosPorFecha.entrySet()) {
            consumoPorDia.add(new Document()
                    .append("fecha", entry.getKey())
                    .append("consumoDia", entry.getValue() * 10));
        }

        return consumoPorDia;
    }


    // Ejemplo de formato: "http://localhost:8080/api/v1/consumo-hora?altura=199&fecha=02/03/2025"
    public List<Document> getConsumoHora(Integer altura, String fechaInput) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

        LocalDate fechaFiltrada;
        try {
            fechaFiltrada = LocalDate.parse(fechaInput, DateTimeFormatter.ofPattern("dd/MM/yyyy"));
        } catch (DateTimeParseException e) {
            return Collections.emptyList();
        }

        FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));
        Map<Integer, Integer> eventosPorHora = new TreeMap<>();

        for (Document evento : eventos) {
            String fechaStr = evento.getString("fecha");
            String horaStr = evento.getString("hora");

            if (fechaStr == null || horaStr == null) continue;

            try {
                LocalDateTime fechaHora = LocalDateTime.parse(fechaStr + " " + horaStr, formatter);
                if (fechaHora.toLocalDate().equals(fechaFiltrada)) {
                    int hora = fechaHora.getHour();
                    eventosPorHora.put(hora, eventosPorHora.getOrDefault(hora, 0) + 1);
                }
            } catch (DateTimeParseException e) {

            }
        }

        List<Document> consumoPorHora = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : eventosPorHora.entrySet()) {
            consumoPorHora.add(new Document()
                    .append("hora", entry.getKey())
                    .append("consumo", entry.getValue() * 10));
        }

        return consumoPorHora;
    }


    public double getCostoEstimadoMensual(Integer altura, int mes, int anio) {
        double tarifa = 10;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

        FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

        int totalEventosDelMes = 0;

        for (Document evento : eventos) {
            String fechaStr = evento.getString("fecha");
            String horaStr = evento.getString("hora");

            try {
                LocalDateTime fechaHora = LocalDateTime.parse(fechaStr + " " + horaStr, formatter);

                if (fechaHora.getMonthValue() == mes && fechaHora.getYear() == anio) {
                    totalEventosDelMes++;
                }
            } catch (DateTimeParseException e) {
            }
        }

        double consumo = totalEventosDelMes * 10;
        return consumo * tarifa;
    }


    public List<Document> getConsumoPorDiaSemanaYMes(Integer altura) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        DateTimeFormatter diaFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
        DateTimeFormatter mesFormatter = DateTimeFormatter.ofPattern("MM/yyyy");

        FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));

        Map<String, Integer> consumoPorDia = new TreeMap<>();
        Map<String, Integer> consumoPorSemana = new TreeMap<>();
        Map<String, Integer> consumoPorMes = new TreeMap<>();

        WeekFields weekFields = WeekFields.of(DayOfWeek.MONDAY, 4);

        for (Document evento : eventos) {
            String fechaStr = evento.getString("fecha");
            String horaStr = evento.getString("hora");

            if (fechaStr == null || horaStr == null) {
                continue;
            }

            try {
                LocalDateTime fechaHora = LocalDateTime.parse(fechaStr + " " + horaStr, formatter);
                int consumoEvento = 10;

                String dia = fechaStr;
                consumoPorDia.put(dia, consumoPorDia.getOrDefault(dia, 0) + consumoEvento);

                int semana = fechaHora.get(weekFields.weekOfWeekBasedYear());
                int anio = fechaHora.get(weekFields.weekBasedYear());
                String semanaKey = String.format("%04d-%02d", anio, semana);
                consumoPorSemana.put(semanaKey, consumoPorSemana.getOrDefault(semanaKey, 0) + consumoEvento);

                String mes = fechaHora.format(mesFormatter);
                consumoPorMes.put(mes, consumoPorMes.getOrDefault(mes, 0) + consumoEvento);

            } catch (DateTimeParseException e) {
                System.out.println("Fecha mal formateada: '" + fechaStr + "' Hora: '" + horaStr + "'");
            }
        }

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

    // Ejemplo de formato: "http://localhost:8080/api/v1/consumo-mensual-alerta?altura=199&fecha=05/2025&limite=11"
    public Document getConsumoPorMesEmitirAlerta(Integer altura, String fechaInput, int limiteMensualKwh) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

        YearMonth mesFiltrado;
        try {
            mesFiltrado = YearMonth.parse(fechaInput, DateTimeFormatter.ofPattern("MM/yyyy"));
        } catch (DateTimeParseException e) {
            return new Document("error", "Formato de fecha inválido. Usar MM/yyyy");
        }

        FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));
        int totalEventos = 0;

        for (Document evento : eventos) {
            String fechaStr = evento.getString("fecha");
            String horaStr = evento.getString("hora");

            if (fechaStr == null || horaStr == null) continue;

            try {
                LocalDateTime fechaHora = LocalDateTime.parse(fechaStr + " " + horaStr, formatter);
                YearMonth eventoMes = YearMonth.from(fechaHora);
                if (eventoMes.equals(mesFiltrado)) {
                    totalEventos++;
                }
            } catch (DateTimeParseException e) {
            }
        }

        int consumoTotal = totalEventos * 10;
        boolean superaLimite = consumoTotal >= (limiteMensualKwh * 0.9);

        Document resultado = new Document()
                .append("mes", mesFiltrado.toString())
                .append("consumoTotal", consumoTotal)
                .append("limiteMensual", limiteMensualKwh)
                .append("alerta", superaLimite ? "El consumo supera el 90% del límite mensual" : "OK");

        return resultado;
    }


    // Ejemplo de formato: "http://localhost:8080/api/v1/escalon-tarifario?altura=199&fecha=02/2025"
    public Map<Integer, Integer> getEscalonTarifarioPorCasa(List<Integer> alturas, String fechaInput){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        YearMonth mesFiltrado;

        try {
            mesFiltrado = YearMonth.parse(fechaInput.trim(), DateTimeFormatter.ofPattern("MM/yyyy"));
        } catch (DateTimeParseException ex) {
            return Collections.emptyMap();
        }

        int limiteEscalon1 = 30;
        int limiteEscalon2 = 60;

        Map<Integer, Integer> escalonesPorCasa = new HashMap<>();

        for (Integer altura : alturas) {
            FindIterable<Document> eventos = eventosCollection.find(Filters.eq("altura", altura));
            int totalEventos = 0;

            for (Document evento : eventos) {
                String fechaStr = evento.getString("fecha");
                String horaStr = evento.getString("hora");
                if (fechaStr == null || horaStr == null) continue;

                try {
                    LocalDateTime fechaHora = LocalDateTime.parse(fechaStr + " " + horaStr, formatter);
                    YearMonth eventoMes = YearMonth.from(fechaHora);
                    if (eventoMes.equals(mesFiltrado)) {
                        totalEventos++;
                    }
                } catch (DateTimeParseException e) {
                }
            }

            int consumoTotal = totalEventos * 10;

            int escalon;
            if (consumoTotal <= limiteEscalon1) {
                escalon = 1;
            } else if (consumoTotal <= limiteEscalon2) {
                escalon = 2;
            } else {
                escalon = 3;
            }

            escalonesPorCasa.put(altura, escalon);
        }

        return escalonesPorCasa;
    }





    public void cerrarConexion() { // cerrar la conexión definitivamente
        if (mongoClient != null) {
            mongoClient.close();
            System.out.println("Conexión cerrada");

        }
    }
}

