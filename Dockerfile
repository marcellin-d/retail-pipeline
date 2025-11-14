FROM astrocrpublic.azurecr.io/runtime:3.1-4

# Créer et installer dans un environnement virtuel Python
RUN python -m venv dbt_venv \
    && dbt_venv/bin/pip install --upgrade pip \
    && dbt_venv/bin/pip install dbt-postgres

# Ajouter le venv au PATH
ENV PATH="/dbt_venv/bin:$PATH"
