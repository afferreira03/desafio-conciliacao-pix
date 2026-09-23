package br.com.desafio.conciliacaopix.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Regras da arquitetura hexagonal: as dependências apontam para dentro (infraestrutura → aplicação → domínio).
 */
class HexagonalArchitectureTest {

    private static final String BASE = "br.com.desafio.conciliacaopix.reconciliation";

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages(BASE);

    @Test
    @DisplayName("Domínio não depende de aplicação, infraestrutura nem de frameworks (Spring, Kafka, Mongo, Jackson)")
    void domainIsFrameworkFree() {
        noClasses().that().resideInAPackage(BASE + ".domain..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        BASE + ".application..",
                        BASE + ".infrastructure..",
                        "org.springframework..",
                        "org.apache.kafka..",
                        "com.mongodb..",
                        "org.bson..",
                        "tools.jackson..",
                        "com.fasterxml.jackson..")
                .check(classes);
    }

    @Test
    @DisplayName("Aplicação não depende de infraestrutura nem de frameworks")
    void applicationDoesNotDependOnInfrastructure() {
        noClasses().that().resideInAPackage(BASE + ".application..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        BASE + ".infrastructure..",
                        "org.springframework..",
                        "org.apache.kafka..",
                        "com.mongodb..",
                        "org.bson..",
                        "io.micrometer..")
                .check(classes);
    }
}