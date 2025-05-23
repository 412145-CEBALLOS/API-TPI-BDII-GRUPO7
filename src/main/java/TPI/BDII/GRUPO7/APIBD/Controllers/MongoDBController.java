package TPI.BDII.GRUPO7.APIBD.Controllers;

import TPI.BDII.GRUPO7.APIBD.Dtos.Casa;
import TPI.BDII.GRUPO7.APIBD.Servicies.MongoDBService;
import jakarta.websocket.server.PathParam;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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
    public void postCasas(@RequestBody Casa casa) {
        bdService.addCasa(casa);
    }

}
