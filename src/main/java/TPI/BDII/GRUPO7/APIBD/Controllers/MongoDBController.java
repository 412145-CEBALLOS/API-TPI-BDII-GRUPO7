package TPI.BDII.GRUPO7.APIBD.Controllers;

import TPI.BDII.GRUPO7.APIBD.Dtos.CasaDTO;
import TPI.BDII.GRUPO7.APIBD.Dtos.EventoDTO;
import TPI.BDII.GRUPO7.APIBD.Servicies.MongoDBService;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@CrossOrigin(origins = "*")
@AllArgsConstructor
@RestController
@RequestMapping("/api/v1")
public class MongoDBController {

    private MongoDBService bdService;

    @GetMapping("/casas") // http://localhost:8080/api/v1/casas
    public List<Document> getCasas() {
        return bdService.getAllCasas();
    }

    @GetMapping("/casas/{altura}")
    public Document getCasasByAltura(@PathVariable("altura") int altura) {
        return bdService.getCasaByAltura(altura);
    }

    @PostMapping("/casas")
    public void postCasas(@RequestBody CasaDTO casaDTO) {
        bdService.addCasa(casaDTO);
    }

    @PostMapping("/eventos")
    public void postCasas(@RequestBody EventoDTO eventoDTO) {
        bdService.addEvento(eventoDTO);
    }

    @GetMapping("/eventos") // http://localhost:8080/api/v1/eventos
    public List<Document> getEventos() {
        return bdService.getAllEventos();
    }

    @GetMapping("/eventos/{altura}") // http://localhost:8080/api/v1/eventos/{altura}
    public List<Document> getEventosByAltura(@PathVariable("altura") int altura) {
        return bdService.getEventosByAltura(altura);
    }

    @GetMapping("/habitacion-mas-usada/{altura}") // http://localhost:8080/api/v1/habitacion-mas-usada/{altura}
    public String getHabitacionMasUsada(@PathVariable("altura") int altura) {
        return bdService.getHabitacionMasUsada(altura);
    }

    @GetMapping("/tiempo-promedio/{altura}") // http://localhost:8080/api/v1/tiempo-promedio/{altura}?sensor={sensor}
    public Integer getTiempoPromedio(@PathVariable("altura") int altura, @RequestParam(name = "sensor") String sensor) {
        return bdService.getTiempoPromedio(altura, sensor);
    }

    @GetMapping("/hora-mas-detecciones/{altura}") // http://localhost:8080/api/v1/hora-mas-detecciones/{altura}
    public String getHoraMasDetecciones(@PathVariable("altura") int altura) {
        return bdService.getHoraMasDetecciones(altura);
    }

    @GetMapping("/ultima-deteccion/{altura}")
    public String getUltimaDeteccionByAltura(@PathVariable("altura") int altura) {
        return bdService.getUltimaDeteccionByAltura(altura);
    }

    @GetMapping("/sensores-sin-deteccion/{altura}")
    public List<String> getSensoresSinDeteccionMes(@PathVariable("altura") int altura) {
        return bdService.getSensoresSinDeteccionMes(altura);
    }

    @GetMapping("/consumo-casa-habitacion/{altura}")
    public Document obtenerConsumoTotalPorCasa(@PathVariable Integer altura) {
        return bdService.getConsumoCasaHabitacion(altura);
    }

    @GetMapping("/top3-casas")
    public List<Document> getTop3CasasConMayorConsumo() {
        return bdService.getTop3CasasMayorConsumo();
    }

    @GetMapping("/consumo-dia")
    public List<Document> getConsumoPorDia(
            @RequestParam Integer altura,
            @RequestParam String fecha) {
        return bdService.getConsumoPorDia(altura, fecha);
    }


    @GetMapping("/consumo-hora")
    public List<Document> getConsumoPorHora(
            @RequestParam Integer altura,
            @RequestParam String fecha) {
        return bdService.getConsumoHora(altura, fecha);
    }

    @GetMapping("/costo-estimado/{altura}/{anio}/{mes}")
    public Double getCostoEstimadoMensual(
            @PathVariable Integer altura,
            @PathVariable int anio,
            @PathVariable int mes) {

        return bdService.getCostoEstimadoMensual(altura, mes, anio);
    }

    @GetMapping("/consumo-dia-semana-mes/{altura}")
    public List<Document> getConsumoPorDiaSemanaMes(@PathVariable Integer altura) {
        return  bdService.getConsumoPorDiaSemanaYMes(altura);
    }

    @GetMapping("/consumo-mensual-alerta")
    public Document getConsumoMensual(
            @RequestParam Integer altura,
            @RequestParam String fecha,
            @RequestParam int limite) {
        return bdService.getConsumoPorMes(altura, fecha, limite);
    }

    @GetMapping("/escalon-tarifario")
    public Map<Integer, Integer> getEscalonTarifario(
            @RequestParam Integer altura,
            @RequestParam String fecha) {
        return bdService.getEscalonTarifarioPorCasa(List.of(altura), fecha);
    }

    //EJ: http://localhost:8080/api/v1/rango-fechas/176?fechaInicio=01/01/2020&fechaFin=30/12/2025
    @GetMapping("/rango-fechas/{altura}")
    public List<Document> getRangoDeFechas(
            @PathVariable int altura,
            @RequestParam String fechaInicio,
            @RequestParam String fechaFin){
        return bdService.getEventosRangoFechas(altura,fechaInicio, fechaFin);
    }

}

