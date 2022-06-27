(ns metabase.models.params.field-values
  "Code related to fetching *cached* FieldValues for Fields to populate parameter widgets. Always used by the field
  values (`GET /api/field/:id/values`) endpoint; used by the chain filter endpoints under certain circumstances."
  (:require [metabase.models.field-values :as field-values :refer [FieldValues]]
            [metabase.models.interface :as mi]
            [metabase.plugins.classloader :as classloader]
            [metabase.public-settings.premium-features :refer [defenterprise]]
            [metabase.util :as u]
            [toucan.db :as db]))

(defn default-get-or-create-field-values-for-current-user!
  "OSS implementation; used as a fallback for the EE implementation if the field isn't sandboxed."
  [field]
  (when (field-values/field-should-have-field-values? field)
    (field-values/get-or-create-field-values! field)))

(defenterprise get-or-create-field-values-for-current-user!*
  "Fetch cached FieldValues for a `field`, creating them if needed if the Field should have FieldValues."
  metabase-enterprise.sandbox.models.params.field-values
  [field]
  (default-get-or-create-field-values-for-current-user! field))

(defn current-user-can-fetch-field-values?
  "Whether the current User has permissions to fetch FieldValues for a `field`."
  [field]
  ;; read permissions for a Field = partial permissions for its parent Table (including EE segmented permissions)
  (mi/can-read? field))

(defn get-or-create-field-values-for-current-user!
  "Fetch FieldValues for a `field`, creating them if needed if the Field should have FieldValues. These are
  filtered as appropriate for the current User, depending on MB version (e.g. EE sandboxing will filter these values).
  If the Field has a human-readable values remapping (see documentation at the top of
  `metabase.models.params.chain-filter` for an explanation of what this means), values are returned in the format

    {:values           [[original-value human-readable-value]]
     :field_id         field-id
     :has_field_values boolean}

  If the Field does *not* have human-readable values remapping, values are returned in the format

    {:values           [[value]]
     :field_id         field-id
     :has_field_values boolean}"
  [field]
  (if-let [field-values (get-or-create-field-values-for-current-user!* field)]
    (-> field-values
        (assoc :values (field-values/field-values->pairs field-values))
        (select-keys [:values :field_id :has_more_values]))
    {:values [], :field_id (u/the-id field), :has_more_values false}))

(defn- create-linked-filter-field-values
  ;; TODO: should this be in params.chain-filter?
  [field constraints hash-key]
  (classloader/require 'metabase.models.params.chain-filter)
  (when-let [values ((resolve 'metabase.models.params.chain-filter/unremapped-chain-filter)
                     (:id field) constraints {})]
    (let [;; If the full FieldValues of this field has a human-readable-values, fix it with the sandboxed values
          human-readable-values (field-values/fixup-human-readable-values
                                  (db/select-one FieldValues
                                                 :field_id (:id field)
                                                 :type :full)
                                  values)]
      (db/insert! FieldValues
                  :field_id (:id field)
                  :type :linked-filter
                  :hash_key hash-key
                  ;; TODO: fill the correct has_more_values
                  :human_readable_values human-readable-values
                  :values values))))

(defenterprise linked-filter-hash-key
  "AAA"
  metabase-enterprise.sandbox.models.params.field-values
  [field constraints]
  (field-values/hash-key-for-linked-filters (:id field) constraints))

(defn get-or-create-linked-filter-field-values!*
  "Returns a sandboxed FieldValues for a field if exists, otherwise try to create one."
  [field constraints]
  (let [hash-key (field-values/hash-key-for-linked-filters (:id field) constraints)
        fv       (or (FieldValues :field_id (:id field)
                                  :type :linked-filter
                                  :hash_key hash-key)
                     (create-linked-filter-field-values field constraints hash-key))]
    (cond
      (nil? fv) nil

      ;; If it's expired, delete then try to re-create it
      (field-values/advanced-fieldvalues-expired? fv) (do
                                                       (db/delete! FieldValues :id (:id fv))
                                                       (recur field constraints))
      :else fv)))

(defn get-or-create-linked-filter-field-values!
  "Fetch FieldValues for a `field`, creating them if needed if the Field should have FieldValues. These are
  filtered as appropriate for the current User, depending on MB version (e.g. EE sandboxing will filter these values).
  If the Field has a human-readable values remapping (see documentation at the top of
  `metabase.models.params.chain-filter` for an explanation of what this means), values are returned in the format

    {:values           [[original-value human-readable-value]]
     :field_id         field-id
     :has_field_values boolean}

  If the Field does *not* have human-readable values remapping, values are returned in the format

    {:values           [[value]]
     :field_id         field-id
     :has_field_values boolean}"
  [field constraints]
  (if-let [field-values (get-or-create-linked-filter-field-values!* field constraints)]
    (-> field-values
        (assoc :values (field-values/field-values->pairs field-values))
        (select-keys [:values :field_id :has_more_values]))
    {:values [], :field_id (u/the-id field), :has_more_values false}))
