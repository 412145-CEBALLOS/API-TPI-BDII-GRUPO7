package TPI.BDII.GRUPO7.APIBD.Dtos;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ConsumoDTO {
    private int escalon;
    private String bonificacion;
    private String alerta;
}
