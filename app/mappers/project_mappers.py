import re
import logging
from typing import Optional
from datetime import datetime

from ..models.user import GestionaleUser, OpenProjectUser, UserStatus, CachedUser
from ..models.project import GestionaleProject, OpenProjectProject, CachedProject
from ..models.base import OpenProjectStatus

logger = logging.getLogger(__name__)


class ProjectMapper:


    def map_gestionale_to_openproject(self, gestionale_project: GestionaleProject) -> OpenProjectProject:

        """
        Map the data from the Gimi database to a project on Openproject,
        """

        try:

            openproject_project = OpenProjectProject(
                name=gestionale_project.NrCommessa,
                identifier=gestionale_project.get_id(),
                codImpianto=gestionale_project.CodImpianto,
                apertura=gestionale_project.get_AperturaCommessa_as_str(),
                fineLavori=gestionale_project.get_FineLavori_as_str(),
                indirizzo=gestionale_project.Indirizzo.format(),
                amministratore=gestionale_project.Ammin.format(),
                fatturazione=gestionale_project.StatoFatturazione,
                note=gestionale_project.Note

            )

            if (gestionale_project.StatoCommessa == 'Aperta'):
                if (gestionale_project.AperturaCommessa > datetime.today()):
                    openproject_project.stato = OpenProjectStatus.NOT_STARTED
            if (gestionale_project.StatoCommessa == 'Chiusa'):
                openproject_project.active = False
                openproject_project.stato = OpenProjectStatus.FINISCHED
            if (gestionale_project.StatoCommessa == 'Sospeso'):
                openproject_project.stato = OpenProjectStatus.DISCONTINUED


            logger.debug(f"Correttamente mappata commessa {gestionale_project.NrCommessa} Gimi -> OpenProject")
            return openproject_project


        except Exception as e:
            logger.error(f"Errore durante il mapping Gimi -> Openproject per commessa {gestionale_project.NrCommessa}: {e}")
            raise


    def map_gestionale_to_cache(self, gestionale_project: GestionaleProject) -> CachedProject:

        """
        Map the data from Gimi database to an entry of the cache (intermedate) database

        """

        try:

            cached_project = CachedProject(
                gestionale_id=gestionale_project.get_id(),
                current_hash=gestionale_project.calculate_hash(),
                sync_status="pending",
                # sync_attempts=0,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                
            )


            logger.debug(f"Correttamente mappata commessa {gestionale_project.NrCommessa} Gimi -> Cache")
            return cached_project


        except Exception as e:
            logger.error(f"Errore durante il mapping Gimi -> Cache per commessa {gestionale_project.NrCommessa}: {e}")
            raise

    
    def update_gestionale_to_cache(self, gestionale_project: GestionaleProject, cached_project: CachedProject) -> bool:

        """
        Map the data from Gimi database to an entry of the cache (intermedate) database

        """

        need_operation = False

        try:

            new_hash = gestionale_project.calculate_hash()

            if cached_project.current_hash != new_hash:
                cached_project.current_hash = new_hash
                cached_project.sync_status = "pending" if cached_project.sync_status != "error" else "error"
                cached_project.updated_at = datetime.now()
                need_operation = True


            logger.debug(f"Correctly updated project {gestionale_project.NrCommessa} Gimi -> Cache")

            return need_operation


        except Exception as e:
            logger.error(f"Errore durante l'aggiornamento Gimi -> Cache per commessa {gestionale_project.NrCommessa}: {e}")
            raise

    
    def mark_sync_success(self, cached_project: CachedProject, op_project: OpenProjectProject):

        try:

            cached_project.openproject_id = op_project.id
            cached_project.last_sync_hash = cached_project.current_hash
            cached_project.updated_at = datetime.now()
            cached_project.last_sync_at = op_project.updated_at
            cached_project.sync_status = "synced"

            logger.debug(f"Sync success for project op_id: {op_project.id}")

        except Exception as e:

            logger.error(f"Error during cache update for project {op_project.id}: {e}")
            raise
    
    def mark_sync_failed(self, cached_project: CachedProject):

        try:

            cached_project.updated_at = datetime.now()
            cached_project.sync_status = "error"

            logger.debug(f"Sync success for project op_id: {cached_project.gestionale_id}")

        except Exception as e:

            logger.error(f"Error during cache update for project {cached_project.gestionale_id}: {e}")
            raise