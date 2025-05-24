package TPI.BDII.GRUPO7.APIBD.Dtos;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@AllArgsConstructor
@Data
public class CasaDTO {
    private int id;
    private int altura;
    private List<String> sensores;
    private ConsumoDTO consumoDTO;
    private int costoMensual;
}
