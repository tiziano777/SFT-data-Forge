## Comportamento attuale

### Le relazioni “derived” sono ON DELETE SET NULL:
dataset.derived_dataset e distribution.derived_from vengono impostate a NULL se il record sorgente viene cancellato (preservi i figli). Vedi tables.sql.

### La relazione dataset → distribution è ON DELETE CASCADE:
distribution.dataset_id è REFERENCES dataset(id) ON DELETE CASCADE, quindi cancellando un dataset verranno eliminate automaticamente tutte le sue distribution. Vedi tables.sql.

### Attenzione: alcuni FK bloccano le cancellazioni anche se esiste cascade/set-null:
strategy.distribution_id è ON DELETE RESTRICT — se esiste una strategy che punta a una distribution, la cancellazione della distribution (anche via cascade) verrà impedita.

Altri riferimenti transitivi (es. strategy_system_prompt, mapping ecc.) hanno comportamenti diversi: mapping.distribution_id è ON DELETE CASCADE (mapping rimossi con la distribution), mentre strategy_system_prompt e strategy.distribution_id usano RESTRICT per proteggere l'integrità delle ricette/strategy.

Nota extra: system_prompt ha un trigger di soft-delete che intercetta DELETE e imposta deleted = TRUE invece di rimuovere fisicamente il record (vedi triggers.sql).

### Interpretazione semantica

SET NULL per relazioni "derived" è conservativo e corretto se vuoi mantenere gli oggetti derivati pur perdendo la provenienza (non perdi i dati figli).

CASCADE per dataset→distribution è corretto se una distribution ha senso solo esistendo nel contesto del dataset padre; però può avere effetti a catena (mapping cancellati, ma cancellazioni potrebbero venire bloccate da FK con RESTRICT).

### Rischi pratici

Cancellare un dataset può fallire se qualche distribution collegata è referenziata da tabelle con RESTRICT (es. strategy).
Cancellazioni automatiche possono eliminare mapping, checkpoint o altri oggetti collegati (controlla ON DELETE per ogni FK).