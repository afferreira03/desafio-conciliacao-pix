package br.com.desafio.conciliacaopix.reconciliation.infrastructure.adapter.out.persistence.mongodb.document;

import br.com.desafio.conciliacaopix.reconciliation.domain.event.DomainEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixInconsistentEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixPendingEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.event.PixReconciledEvent;
import br.com.desafio.conciliacaopix.reconciliation.domain.model.vo.EndToEndId;
import com.fasterxml.uuid.Generators;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Objects;

@Document(collection = "outbox_events")
@CompoundIndex(name = "idx_outbox_pending_createdAt", def = "{'status': 1, 'createdAt': 1}",
        partialFilter = "{ 'status' : 'PENDING' }")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEventDocument {

    @Id
    private String id;
    private String endToEndId;
    private String eventType;
    private String payload;
    private OutboxEventStatus status;
    private Instant createdAt;
    private Instant sentAt;

    public static OutboxEventDocument fromDomain(DomainEvent event, String payloadJson) {
        EventDescriptor eventDescriptor = switch (event) {
            case PixReconciledEvent e -> new EventDescriptor(e.endToEndId(), "PixReconciledEvent");
            case PixInconsistentEvent e -> new EventDescriptor(e.endToEndId(), "PixInconsistentEvent");
            case PixPendingEvent e -> new EventDescriptor(e.endToEndId(), "PixPendingEvent");
        };

        return OutboxEventDocument.builder()
                .id(Generators.timeBasedEpochGenerator().generate().toString())
                .endToEndId(eventDescriptor.endToEndId().value())
                .eventType(eventDescriptor.eventName())
                .payload(payloadJson)
                .status(OutboxEventStatus.PENDING)
                .createdAt(Instant.now())
                .build();
    }

    private record EventDescriptor(EndToEndId endToEndId, String eventName) {
        public EventDescriptor {
            Objects.requireNonNull(endToEndId);
            Objects.requireNonNull(eventName);
        }
    }
}