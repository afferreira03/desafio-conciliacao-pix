package br.com.desafio.conciliacaopix.architecture;

import br.com.desafio.conciliacaopix.ConciliacaoPixApplication;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.modulith.core.ApplicationModules;
import org.springframework.modulith.docs.Documenter;

/**
 * Verifica as fronteiras do monólito modular (Spring Modulith) e gera a documentação dos módulos
 * (diagramas C4/PlantUML e canvas) em {@code target/spring-modulith-docs}.
 */
class ModularityTest {

    private final ApplicationModules modules = ApplicationModules.of(ConciliacaoPixApplication.class);

    @Test
    @DisplayName("Módulos não devem ter ciclos nem acessar tipos internos de outros módulos")
    void verifiesModularStructure() {
        modules.verify();
    }

    @Test
    @DisplayName("Deve gerar a documentação dos módulos")
    void writesDocumentation() {
        new Documenter(modules)
                .writeModulesAsPlantUml()
                .writeIndividualModulesAsPlantUml()
                .writeModuleCanvases();
    }
}