package TPI.BDII.GRUPO7.APIBD.Servicies;

import TPI.BDII.GRUPO7.APIBD.DBconection.MongoDBManager;
import TPI.BDII.GRUPO7.APIBD.Dtos.CasaDTO;
import TPI.BDII.GRUPO7.APIBD.Dtos.EventoDTO;
import jakarta.annotation.PreDestroy;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class MongoDBService {

    private final MongoDBManager dbManager;
    public MongoDBService(){
        this.dbManager = new MongoDBManager();
    }

    @PreDestroy
    public void cleanup(){
        if(dbManager != null){
            dbManager.cerrarConexion();
        }
    }
    public List<Document> getAllCasas() {
        return dbManager.getAllCasas();
    }

    public Document getCasaByAltura(int altura) {
        return dbManager.getCasasByAltura(altura);
    }

    public void addCasa(CasaDTO casaDTO) {
        dbManager.crearCasa(casaDTO);
    }

    public void addEvento(EventoDTO eventoDTO) {
        dbManager.crearEvento(eventoDTO);
    }

    public List<Document> getAllEventos() {
        return dbManager.getAllEventos();
    }

    public List<Document> getEventosByAltura(int altura) {
        return dbManager.getEventosByAltura(altura);
    }

    public String getHabitacionMasUsada(int altura) {
        return dbManager.getHabitacionMasUsada(altura);
    }

    public Integer getTiempoPromedio(int altura, String sensor) {
        return dbManager.getTiempoPromedio(altura, sensor);
    }

    public String getHoraMasDetecciones(int altura) {
        return dbManager.getHoraMasDetecciones(altura);
    }

    public String getUltimaDeteccionByAltura(int altura) {
        return dbManager.getUltimaDeteccionByAltura(altura);
    }

    public List<String> getSensoresSinDeteccionMes(int altura) {
        return dbManager.getSensoresSinDeteccionMes(altura);
    }

    public Document getConsumoCasaHabitacion(Integer altura) {
        return dbManager.getConsumoCasaHabitacion(altura);
    }

    public List<Document> getTop3CasasMayorConsumo() {
        return dbManager.getTop3CasasMayorConsumo();
    }

    public List<Document> getConsumoPorDia(Integer altura, String fechaInput) {
        return dbManager.getConsumoPorDia(altura, fechaInput);
    }

    public List<Document> getConsumoHora(Integer altura, String fecha) {
        return dbManager.getConsumoHora(altura, fecha);
    }


    public double getCostoEstimadoMensual(Integer altura, int mes, int anio) {
        return dbManager.getCostoEstimadoMensual(altura, mes, anio);
    }

    public List<Document> getConsumoPorDiaSemanaYMes (Integer altura){
        return dbManager.getConsumoPorDiaSemanaYMes(altura);
    }

    public Document getConsumoPorMes(Integer altura, String fechaInput, int limiteMensualKwh) {
        return dbManager.getConsumoPorMesEmitirAlerta(altura, fechaInput, limiteMensualKwh);
    }
}
