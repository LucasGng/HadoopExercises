package advanced.Recuperacao;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class ContagemMatriculas implements WritableComparable<advanced.Recuperacao.ContagemMatriculas> {

    private int matriculas;
    private int contagem;

    public ContagemMatriculas() {
        this.matriculas = 0;
        this.contagem = 0;
    }

    public ContagemMatriculas(int matriculas, int contagem) {
        this.matriculas = matriculas;
        this.contagem = contagem;
    }

    public int getMatriculas() {
        return matriculas;
    }

    public void setMatriculas(int matriculas) {
        this.matriculas = matriculas;
    }

    public int getContagem() {
        return contagem;
    }

    public void setContagem(int contagem) {
        this.contagem = contagem;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        advanced.Recuperacao.ContagemMatriculas that = (advanced.Recuperacao.ContagemMatriculas) o;
        return Objects.equals(matriculas, that.matriculas) && Objects.equals(contagem, that.contagem);
    }

    @Override
    public int hashCode() {
        return Objects.hash(matriculas, contagem);
    }

    @Override
    public String toString() {
        return matriculas + "\t" + contagem;
    }

    @Override
    public int compareTo(advanced.Recuperacao.ContagemMatriculas o) {
        if (hashCode() < o.hashCode()) {
            return -1;
        } else if (hashCode() > o.hashCode()) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(matriculas);
        dataOutput.writeInt(contagem);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        matriculas = dataInput.readInt();
        contagem = dataInput.readInt();
    }
}
