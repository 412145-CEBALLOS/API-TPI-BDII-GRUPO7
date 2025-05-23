package TPI.BDII.GRUPO7.APIBD.Servicies;

import TPI.BDII.GRUPO7.APIBD.DBconection.MongoDBManager;
import TPI.BDII.GRUPO7.APIBD.Dtos.Casa;
import lombok.AllArgsConstructor;
import org.bson.Document;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class MongoDBService {

    private final MongoDBManager dbManager = new MongoDBManager();

    public List<Document> getAllCasas() {
        return dbManager.obtenerTodasLasCasas();
    }

    public Document getCasaByAltura(int altura) {
        return dbManager.obtenerCasasPorAltura(altura);
    }

    public void addCasa(Casa casa) {
        dbManager.crearCasa(casa);
    }

}
