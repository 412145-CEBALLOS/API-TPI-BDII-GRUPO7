package TPI.BDII.GRUPO7.APIBD.Dtos;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EventoDTO {
    private int id;
    private int altura;
    private String sensor;
    private String fecha;
    private String hora;
    private Integer segundos;
}
