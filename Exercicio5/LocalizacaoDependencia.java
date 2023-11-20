package advanced.Recuperacao;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class LocalizacaoDependencia implements WritableComparable<advanced.Recuperacao.LocalizacaoDependencia> {

    private String localizacao;
    private String dependencia;

    public LocalizacaoDependencia() {
        this.localizacao = null;
        this.dependencia = null;
    }

    public LocalizacaoDependencia(String localizacao, String dependencia) {
        this.localizacao = localizacao;
        this.dependencia = dependencia;
    }

    public String getLocalizacao() {
        return localizacao;
    }

    public void setLocalizacao(String localizacao) {
        this.localizacao = localizacao;
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
        advanced.Recuperacao.LocalizacaoDependencia that = (advanced.Recuperacao.LocalizacaoDependencia) o;
        return Objects.equals(localizacao, that.localizacao) && Objects.equals(dependencia, that.dependencia);
    }

    @Override
    public int hashCode() {
        return Objects.hash(localizacao, dependencia);
    }

    @Override
    public String toString() {
        return localizacao + "\t" + dependencia;
    }

    @Override
    public int compareTo(advanced.Recuperacao.LocalizacaoDependencia o) {
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
        dataOutput.writeUTF(localizacao);
        dataOutput.writeUTF(dependencia);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        localizacao = dataInput.readUTF();
        dependencia = dataInput.readUTF();
    }
}
