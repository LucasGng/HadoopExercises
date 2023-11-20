package advanced.Recuperacao;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class SiglaDependencia implements WritableComparable<advanced.Recuperacao.SiglaDependencia> {

    private String sigla;
    private String dependencia;

    public SiglaDependencia() {
        this.sigla = null;
        this.dependencia = null;
    }

    public SiglaDependencia(String sigla, String dependencia) {
        this.sigla = sigla;
        this.dependencia = dependencia;
    }

    public String getSigla() {
        return sigla;
    }

    public void setSigla(String sigla) {
        this.sigla = sigla;
    }

    public String getDependencia() {
        return dependencia;
    }

    public void setDependencia(String Dependencia) {
        this.dependencia = dependencia;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        advanced.Recuperacao.SiglaDependencia that = (advanced.Recuperacao.SiglaDependencia) o;
        return Objects.equals(sigla, that.sigla) && Objects.equals(dependencia, that.dependencia);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sigla, dependencia);
    }

    @Override
    public String toString() {
        return sigla + "\t" + dependencia;
    }

    @Override
    public int compareTo(advanced.Recuperacao.SiglaDependencia o) {
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
        dataOutput.writeUTF(sigla);
        dataOutput.writeUTF(dependencia);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sigla = dataInput.readUTF();
        dependencia = dataInput.readUTF();
    }
}
