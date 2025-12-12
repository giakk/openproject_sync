import logging
from datetime import datetime

from ..models.user import GestionaleUser, OpenProjectUser, CachedUser

logger = logging.getLogger(__name__)

class UserMapper:

    def map_gestionale_to_openproject_user(self, Gimi_manutentore: GestionaleUser) -> OpenProjectUser:

        try:

            name, lastname = Gimi_manutentore.extract_first_and_last_name()

            openproject_user = OpenProjectUser(
                firstName=name,
                lastName=lastname,
                email=Gimi_manutentore.getEmail(),
                phone=Gimi_manutentore.getPhone(),
            )

            logger.debug(f"Correttamente mappato utente {Gimi_manutentore.email} Gimi -> OpenProject")
            return openproject_user
        
        except Exception as e:

            logger.error(f"Errore durante il mapping Gimi -> Openproject per usente {Gimi_manutentore.email}: {e}")
            raise

    def map_gestionale_to_cache_user(self, Gimi_manutentore: GestionaleUser) -> CachedUser:

        try:

            cached_user = CachedUser(
                gestionale_id=Gimi_manutentore.GimiId,
                current_hash=Gimi_manutentore.calculate_hash(),
                email=Gimi_manutentore.getEmail(),
                sync_status="pending",
                created_at=datetime.now(),
                updated_at=datetime.now()
            )

            logger.debug(f"Correttamente mappato utente {Gimi_manutentore.email} Gimi -> Cache")
            return cached_user


        except Exception as e:
            logger.error(f"Errore durante il mapping Gimi -> Cache per usente {Gimi_manutentore.email}: {e}")
            raise


    def update_gestionale_user_to_cache(self, Gimi_manutentore: GestionaleUser, cached_user: CachedUser) -> bool:

        need_operation = False

        try:

            new_hash = Gimi_manutentore.calculate_hash()

            if cached_user.current_hash != new_hash:
                cached_user.current_hash = new_hash
                cached_user.sync_status = "pending" if cached_user.sync_status != "error" else "error"
                cached_user.updated_at = datetime.now()
                need_operation = True


            logger.debug(f"Correctly updated user {Gimi_manutentore.email} Gimi -> Cache")

            return need_operation


        except Exception as e:
            logger.error(f"Errore durante l'aggiornamento Gimi -> Cache per utente {Gimi_manutentore.email}: {e}")
            raise

    def mark_sync_success_user(self, cached_user: CachedUser, op_user: OpenProjectUser):

        try:

            cached_user.openproject_id = op_user.id
            cached_user.last_sync_hash = cached_user.current_hash
            cached_user.updated_at = op_user.updated_at
            cached_user.email = op_user.email
            cached_user.last_sync_at = datetime.now()
            cached_user.sync_status = "synced"

            logger.debug(f"Sync success for user with ID: {op_user.id}")

        except Exception as e:

            logger.error(f"Error during cache update for user {op_user.id}: {e}")
            raise
    
    def mark_sync_failed_user(self, cached_user: CachedUser):

        try:

            cached_user.updated_at = datetime.now()
            cached_user.sync_status = "error"

            logger.debug(f"Sync failed for user with ID: {cached_user.gestionale_id}")

        except Exception as e:

            logger.error(f"Error during cache update for user {cached_user.gestionale_id}: {e}")
            raise

