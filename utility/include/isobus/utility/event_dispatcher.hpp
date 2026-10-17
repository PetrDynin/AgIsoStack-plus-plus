//================================================================================================
/// @file event_dispatcher.hpp
///
/// @brief An object to represent a dispatcher that can invoke callbacks in a thread-safe manner.
/// @author Daan Steenbergen
///
/// @copyright 2024 The Open-Agriculture Developers
//================================================================================================
#ifndef EVENT_DISPATCHER_HPP
#define EVENT_DISPATCHER_HPP

#include "isobus/utility/thread_synchronization.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#if !defined CAN_STACK_DISABLE_THREADS && !defined ARDUINO
#include <queue>
#else
#include <vector>
#endif
#include <unordered_map>

namespace isobus
{
	using EventCallbackHandle = std::size_t;

	/// @brief A dispatcher that notifies listeners when an event is invoked.
	template<typename... E>
	class EventDispatcher
	{
	public:
		using Callback = std::function<void(const E &...)>;

		/// @brief Register a callback to be invoked when the event is invoked.
		/// @param callback The callback to register.
		/// @return A unique identifier for the callback, which can be used to remove the listener.
		EventCallbackHandle add_listener(const Callback &callback)
		{
			EventCallbackHandle id = nextId;
			nextId += 1;

			LOCK_GUARD(Mutex, callbacksMutex);
			if (isExecuting)
			{
#if !defined CAN_STACK_DISABLE_THREADS && !defined ARDUINO
				modifications.push([this, id, callback]() { callbacks[id] = callback; });
#else
				modifications.emplace_back([this, id, callback]() { callbacks[id] = callback; });
#endif
			}
			else
			{
				callbacks[id] = callback;
			}
			return id;
		}

		/// @brief Register a callback to be invoked when the event is invoked.
		/// @param callback The callback to register.
		/// @param context The context object to pass through to the callback.
		/// @return A unique identifier for the callback, which can be used to remove the listener.
		template<typename C>
		EventCallbackHandle add_listener(const std::function<void(const E &..., std::shared_ptr<C>)> &callback, std::weak_ptr<C> context)
		{
			Callback callbackWrapper = [callback, context](const E &...args) {
				if (auto contextPtr = context.lock())
				{
					callback(args..., contextPtr);
				}
			};
			return add_listener(callbackWrapper);
		}

		/// @brief Register an unsafe callback to be invoked when the event is invoked.
		/// @param callback The callback to register.
		/// @param context The context object to pass through to the callback.
		/// @return A unique identifier for the callback, which can be used to remove the listener.
		template<typename C>
		EventCallbackHandle add_unsafe_listener(const std::function<void(const E &..., C *)> &callback, C *context)
		{
			Callback callbackWrapper = [callback, context](const E &...args) {
				callback(args..., context);
			};
			return add_listener(callbackWrapper);
		}

		/// @brief Remove a callback from the list of listeners.
		/// @param id The unique identifier of the callback to remove.
		void remove_listener(EventCallbackHandle id) noexcept
		{
			LOCK_GUARD(Mutex, callbacksMutex);
			if (isExecuting)
			{
#if !defined CAN_STACK_DISABLE_THREADS && !defined ARDUINO
				modifications.push([this, id]() { callbacks.erase(id); });
#else
				modifications.emplace_back([this, id]() { callbacks.erase(id); });
#endif
			}
			else
			{
				callbacks.erase(id);
			}
		}

		/// @brief Remove all listeners from the event.
		void clear_listeners() noexcept
		{
			LOCK_GUARD(Mutex, callbacksMutex);
			if (isExecuting)
			{
#if !defined CAN_STACK_DISABLE_THREADS && !defined ARDUINO
				modifications.push([this]() { callbacks.clear(); });
#else
				modifications.emplace_back([this]() { callbacks.clear(); });
#endif
			}
			else
			{
				callbacks.clear();
			}
		}

		/// @brief Get the number of listeners registered to this event.
		/// @return The number of listeners
		std::size_t get_listener_count()
		{
			LOCK_GUARD(Mutex, callbacksMutex);
			return callbacks.size();
		}

		/// @brief Call and event with context that is forwarded to all listeners.
		/// @param args The event context to notify listeners with.
		/// @return True if the event was successfully invoked, false otherwise.
		void invoke(E &&...args)
		{
			call(args...);
		}

		/// @brief Call an event with existing context to notify all listeners.
		/// @param args The event context to notify listeners with.
		/// @return True if the event was successfully invoked, false otherwise.
		void call(const E &...args)
		{
			{
				// Set flag to indicate we will be reading the list of callbacks, and
				// prevent other threads from modifying the list directly during this time
				LOCK_GUARD(Mutex, callbacksMutex);
				isExecuting = true;
			}

			// Execute the callbacks
			for (const auto &callback : callbacks)
			{
				callback.second(args...);
			}

			{
				LOCK_GUARD(Mutex, callbacksMutex);
				isExecuting = false;

				// Apply pending modifications to the callbacks list
				while (!modifications.empty())
				{
					auto modification = std::move(modifications.front());
#if !defined CAN_STACK_DISABLE_THREADS && !defined ARDUINO
					modifications.pop();
#else
					modifications.erase(modifications.begin());
#endif
					modification();
				}
			}
		}

	private:
		std::unordered_map<EventCallbackHandle, Callback> callbacks; ///< The list of callbacks
		bool isExecuting = false; ///< Whether the dispatcher is currently executing an event
#if !defined CAN_STACK_DISABLE_THREADS && !defined ARDUINO
		std::queue<std::function<void()>> modifications; ///< The modifications to the callbacks list
#else
		std::vector<std::function<void()>> modifications; ///< The modifications to the callbacks list
#endif
		Mutex callbacksMutex; ///< The mutex to protect the object from (unwanted) concurrent access
		EventCallbackHandle nextId = 0; // Counter for generating unique IDs
	};
} // namespace isobus

#endif // EVENT_DISPATCHER_HPP
