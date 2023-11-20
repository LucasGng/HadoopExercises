package advanced.Recuperacao;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class NomeMunicipio implements WritableComparable<advanced.Recuperacao.NomeMunicipio> {

    private String nome;
    private String municipio;

    public NomeMunicipio() {
        this.nome = null;
        this.municipio = null;
    }

    public NomeMunicipio(String nome, String municipio) {
        this.nome = nome;
        this.municipio = municipio;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public String getMunicipio() {
        return municipio;
    }

    public void setMunicipio(String municipio) {
        this.municipio = municipio;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        advanced.Recuperacao.NomeMunicipio that = (advanced.Recuperacao.NomeMunicipio) o;
        return Objects.equals(nome, that.nome) && Objects.equals(municipio, that.municipio);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nome, municipio);
    }

    @Override
    public String toString() {
        return nome + "\t" + municipio;
    }

    @Override
    public int compareTo(advanced.Recuperacao.NomeMunicipio o) {
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
        dataOutput.writeUTF(nome);
        dataOutput.writeUTF(municipio);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        nome = dataInput.readUTF();
        municipio = dataInput.readUTF();
    }
}
